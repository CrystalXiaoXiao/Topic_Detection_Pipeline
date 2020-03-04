import time
import sys
import os
import traceback
from pprint import pprint
import os
import ast
import shutil
from functools import partial
import argparse
import ujson
from pprint import pprint

from pykafka import KafkaClient, BalancedConsumer, Cluster
from pykafka.exceptions import ConsumerStoppedException
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, RDD, SparkConf, StorageLevel
from functools import reduce
from collections import defaultdict
from collections.abc import Iterable
from itertools import chain
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError,NotFoundError,RequestError
import numpy as np
import math
from Utils.utils import get_imp_words, get_noun_words_and_ngrams

stop_words_list = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]

def process_stream(stream=None,context=None,hosts=None,zookeeper_hosts=None,save_to_topic=None,es_index_name=None):
    if stream is None:
        raise Exception("No stream provided")
    
    extract_imp_words = context.sparkContext.broadcast(get_imp_words)
    extract_noun_ngrams = context.sparkContext.broadcast(get_noun_words_and_ngrams)
    stop_words = context.sparkContext.broadcast(stop_words_list)

    def infer_and_save_to_kafka(rdd,zookeeper_hosts,topic,es_index_name):
        rdd.foreachPartition(lambda part_records: infer_topics(part_records,zookeeper_hosts,topic,es_index_name))
    
    def save_inferred_to_kafka(messages, zookeeper_hosts, topic):
        print(f"Received {len(messages)} messages for saving in kafka")
        k_client = KafkaClient(zookeeper_hosts=zookeeper_hosts)
        with k_client.topics[topic].get_producer(sync=True) as producer:
            for bytes_msg in messages:
                # print("Producing Message", str(bytes_msg))
                producer.produce(bytes_msg)
            producer.stop()
    
    def infer_topics(precs, zk_hosts, kafka_topic, index_name):
        es = None
        es_index_name = index_name
        try:
            es = Elasticsearch(
                # ['elasticsearch'],
            # sniff before doing anything
            sniff_on_start=True,
            # refresh nodes after a node fails to respond
            sniff_on_connection_fail=True,
            # and also every 60 seconds
            sniffer_timeout=60)
            es.search(index=es_index_name, filter_path=['hits.hits._source'])
        except TransportError:
            do_this_on_exception(TransportError,"Elasticsearch Unreachable :(",traceback.format_exc())
        except NotFoundError:
            do_this_on_exception(NotFoundError,f"Topic - {es_index_name} not found :(",traceback.format_exc())
        except:
            do_this_on_exception(Exception, "Error Occurred!", traceback.format_exc())
        
        if not es:
            return
            
        kafka_messages = []
        for line in precs:
            line = line[1] # incoming rdd record is a KV tuple
            words_stat_dict = defaultdict(dict)
            title, text, tags = '','',[]
            try:
                line = ujson.loads(line)
                title = line.get('title','')
                text = line.get('text','')
                tags = [tag.lower() for tag in line.get('tags','').split('|TAG|')]
            except:
                continue
            f = extract_imp_words.value # fetch the broadcast function
            ex_ng = extract_noun_ngrams.value
            # print(f"\nFETCHING IMP WORDS FOR LINE:\n{text}\n")
            from collections import Counter
            
            imp_words_stats = f(text, title)
            title_noun_ngrams = ex_ng(title)
            text_noun_ngrams = ex_ng(text)
            imp_word_freqs = dict([(tup[0],tup[1].get('total_freq', 0)) for tup in imp_words_stats.items()])
            all_UNQ_story_words_freqs = Counter([word.lower().strip('.,/\\\'“”@#') for word in text.split(' ')])
            all_UNQ_title_words_freqs = Counter([word.lower().strip('.,/\\\'“”@#') for word in title.split(' ')])
            
            for ngram in title_noun_ngrams:
                words_stat_dict[ngram]['in_title'] = True
            
            for ngram in text_noun_ngrams:
                words_stat_dict[ngram]['in_story'] = True

            for word in imp_word_freqs.keys():
                words_stat_dict[word]['freq'] = imp_word_freqs[word]
                words_stat_dict[word]['is_imp'] = True
            for word in all_UNQ_story_words_freqs.keys():
                if not words_stat_dict[word].get('freq', None): # may have been populated before by imp_word_freqs dict
                    words_stat_dict[word]['freq'] = all_UNQ_story_words_freqs[word]
                words_stat_dict[word]['in_story'] = True
                words_stat_dict[word]['in_story_freq'] = all_UNQ_story_words_freqs[word]
            for word in all_UNQ_title_words_freqs.keys():
                if not words_stat_dict[word].get('freq', None): # may have been populated before by imp_word_freqs dict
                    words_stat_dict[word]['freq'] = all_UNQ_title_words_freqs[word]
                words_stat_dict[word]['in_title'] = True
                words_stat_dict[word]['in_title_freq'] = all_UNQ_title_words_freqs[word]
            for word in tags:
                words_stat_dict[word]['is_tag'] = True

            topics = query_es(es, index_name, words_stat_dict)
            # print(f"Topics inferred for: \n{line}:")
            # print(f"{topics}\n\n")
            to_kafka_record = line
            print(f"Title: {title}, Topics: {topics}")
            if len(topics) > 0:
                to_kafka_record['topics'] = topics
            else:
                to_kafka_record['topics'] = {}
            record_bytes = bytes(ujson.dumps(to_kafka_record), encoding='utf-8')
            kafka_messages.append(record_bytes)
        if len(kafka_messages) > 0:
            print(f"Sending {len(kafka_messages)} messages for saving in kafka")
            save_inferred_to_kafka(kafka_messages, zk_hosts, kafka_topic)


    def query_es(es=None, index=None, words_stat_dict={}):
        
        def fetch_es(es, body, index, from_, size, filter_path):
            es_results = None
            try:
                es.search( index=index, filter_path=filter_path )
                es_results = es.search(
                    body = body,
                    index = [index,],
                    # _source = ['topic','word'], 
                    from_ = from_,
                    size=size,
                    filter_path=filter_path
                )
            except RequestError:
                do_this_on_exception(RequestError,"Malformed ES Request",traceback.format_exc())
            except TransportError:
                do_this_on_exception(TransportError,"Elasticsearch Unreachable :(",traceback.format_exc())
            except NotFoundError:
                do_this_on_exception(NotFoundError,f"Topic - {index} not found :(",traceback.format_exc())
            except:
                do_this_on_exception(Exception,"Error Occurred!",traceback.format_exc())
            return es_results

        qwords = words_stat_dict.keys()
        qwords = [word.lower() for word in qwords if word not in stop_words.value]
        print(f"{len(qwords)} Query Terms")
        pprint(qwords)
        query = {
            "query":{
                "terms": {
                "word": qwords
                }
            }
        }

        # DEBUG: print(f"\nStory#{story_counter}\nQuery for words\n",qwords)
        query = ujson.dumps(query)
        topic_strengths = defaultdict(float)
        words_also_topics = set()
        word_topic_inverted_index = defaultdict(set)
        topic_word_inverted_index = defaultdict(set)
        from_counter = 1
        results_batch_size = 100
        from_pos = int( from_counter / results_batch_size ) * results_batch_size
        es_results = fetch_es(es,query, index, from_pos, results_batch_size, ['hits.hits._source'])

        iteration = 1
        while(es_results and len(es_results.get('hits',{}).get('hits', [])) > 0):
            print(f"Results from {from_pos}-{from_pos + results_batch_size}")
            hits = es_results.get("hits", {}).get("hits",None)
            
            print(f"{len(hits)} hits for story in {iteration}, parsing..")
            # print("hits")
            # print(hits)
            if hits and isinstance(hits, list) and len(hits) > 0:
                for idx, hit in enumerate(hits):
                    src = hit['_source']
                    # print(f"iteration {iteration}, Hit {idx}:")
                    # print(src)
                    topic_origcase,topic,word = src.get("topic_origcase", None),src.get("topic", None),src.get("word", None)
                    if word.lower().strip() in [topic, '_'.join(topic_origcase.lower().split(' ')), topic_origcase.lower()]:
                        words_also_topics.add(word)
                    word_topic_inverted_index[word].add(topic)
                    topic_word_inverted_index[topic].add(word)
                    print(f'topic: {src.get("topic", None)} for word {src.get("word", None)}')
            
            from_counter += results_batch_size
            from_pos = int( from_counter / results_batch_size ) * results_batch_size
            es_results = None
            es_results = fetch_es(es, query, index, from_pos, results_batch_size, ['hits.hits._source'])
            iteration += 1

        # To rank topics, we need:
        # 1. word-count for the word in story text
        # 2. see if word occurs as a topic itself
        # 3. see how general a word is using a generality score
        # 4. check if word is a tag (provided by the publisher)
        # 5. check if wordoccurs in the title
        # Note: no algo, just naive boosting factor used for this demo. More involved/learning-to-rank methods can be used to compute topic scores
        for topic in topic_word_inverted_index:
            for word in topic_word_inverted_index[topic]:
                w_count = words_stat_dict[word].get('freq',0)
                word_strength = 0
                word_strength = math.log( max( 2, w_count ), 2) # 1.
                if words_stat_dict[word].get('in_title',False):
                    word_strength *= 1.5 # boost the strength
                if words_stat_dict[word].get('is_tag',False):
                    word_strength *= 1.5 # boost the strength
                words_also_topics = list(words_also_topics) # 2.
                if word in words_also_topics:
                    word_strength *= 1.5 # boost the strength
                
                # word's topic-generality (w_tgen below) score i.e. how many topics does the word appear in.
                # It could be indicative of a more general word than a specific (and thus more specific) word; lower score is better
                w_tgen = max([1,len(word_topic_inverted_index.get(word,[]))]) # 3.
                # w_tgen would be >= 1
                # more w_tgen, less specific, less useful as a measure of theme os an article, so less contribution to score
                # i.e. w_tgen and score have an inverse relationship
                # word_strength += 1 / w_tgen
                word_strength /= w_tgen
                topic_strengths[topic] += word_strength
        
        if len(topic_strengths) > 0:
            topic_strengths = dict(sorted(dict(topic_strengths).items(),key=lambda x:x[1],reverse=True))
        
        print(f"Topics: {','.join(topic_strengths.keys())}")
        return topic_strengths
    
    stream.foreachRDD(lambda rdd: infer_and_save_to_kafka(rdd, zookeeper_hosts, save_to_topic, es_index_name))

def create_streaming_context():
    conf = SparkConf()
    pairs = [
        ('spark.app.name','Process Stories Stream'),
        ('spark.master','local[4]'),
        ('spark.ui.port', '4040')
    ]
    conf.setAll(pairs)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, batch_secs)
    ssc.checkpoint(checkpointDirectory)  # set checkpoint directory
    return ssc

def do_this_on_exception(exception_type=None,msg=None,traceback_output=None,print_to_stdout=None):
    if print_to_stdout is None:
        print_to_stdout = True # may be helpful when testing in 'local' spark mode
    if msg is not None:
        if print_to_stdout:
            print(msg)
    if traceback_output is not None:
        if print_to_stdout:
            print(traceback_output)
    if exception_type is None:
        if isinstance(exception_type, RequestError):
            raise
        elif isinstance(exception_type, TransportError):
            raise
        elif isinstance(exception_type, NotFoundError):
            raise
        elif isinstance(exception_type, Exception):
            raise
        else:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('-t','--topic')
    parser.add_argument('-s','--savetopic')
    parser.add_argument('-e','--esindex')
    parser.add_argument('-c','--chkpdir')
    parser.add_argument('-b','--batchsec')
    parser.add_argument('-k','--kfkhosts')
    parser.add_argument('-z','--zkhosts')
    
    args = parser.parse_args()
    topic_to_consume = None
    save_to_topic = None
    es_index = None
    zookeeper_hosts = None
    kafka_hosts = None
    checkpointDirectory = './checkpoint'
    batch_secs = 5 # default value
    print("Params received:\n")
    print(args)
    
    if not (args.topic or isinstance(args.topic, str)):
        raise Exception("Please provide name of kafka topic to consume stories from (using the -t or --topic flag)")
    else:
        topic_to_consume = args.topic
    
    if not (args.savetopic or isinstance(args.savetopic, str)):
        raise Exception("Please provide name of kafka topic to save processed stories in (using the -s or --savetopic flag)")
    else:
        save_to_topic = args.savetopic
    
    if not (args.esindex or isinstance(args.esindex, str)):
        raise Exception("Please provide name of elasticsearch index to query (using the -e or --esindex flag)")
    else:
        es_index = args.esindex

    if not (args.kfkhosts or isinstance(args.kfkhosts, str)):
        raise Exception("Please provide a comma separated string of Kafka hosts (using the -k or --kfkhosts flag)")
    else:
        kafka_hosts = args.kfkhosts

    if not (args.zkhosts or isinstance(args.zkhosts, str)):
        raise Exception("Please provide a comma separated string of Zookeeper hosts (using the -z or --zkhosts flag)")
    else:
        zookeeper_hosts = args.zkhosts
    
    if not (args.chkpdir or isinstance(args.chkpdir, str)):
        raise Exception("Please provide path (absolute or relative using ./)for the checkpoint directory")
    else:
        if not args.chkpdir == '':
            checkpointDirectory = args.chkpdir
    
    if not args.batchsec:
        raise Exception("Please provide time (in secs) for constructing DStream batches (using the -b or --batchsec flag)")
    else:
        try:
            batch_secs = int(args.batchsec)
        except:
            print(f"Invalid value for dstream batch length (in secs). Expected: integer, found {args.batchsec}\nUsing default value of 2")
            pass
    

    print("Trying to establish a connection to kafka...")
    k_client = KafkaClient(zookeeper_hosts=zookeeper_hosts)
    print("Connection established.")
    
    try:
        topic_exists = False
        for topic in k_client.topics:
            if str(topic,'utf-8') == topic_to_consume:
                topic_object = k_client.topics[topic]
                topic_exists = True
                break
        if not topic_exists:
            print(f"Topic - {topic_to_consume} - does not exist in Kafka")
            raise Exception(f"Topic - {topic_to_consume} - does not exist in Kafka")
        else:
            print(f"Consuming from {topic_to_consume}")
            
            brokers = kafka_hosts
            
            # [ ** LOCAL MODE ONLY ** ] : Try to delete the checkpoint directory if it already exists in, to avoid directory clutter and meddling with earlier checkpointing
            try:
                if os.path.exists(os.path.abspath(checkpointDirectory)):
                    shutil.rmtree(os.path.abspath(checkpointDirectory))
            except:
                pass

            context = StreamingContext.getOrCreate(checkpointDirectory, create_streaming_context)
            directKafkaStream = KafkaUtils.createDirectStream(context, [topic_to_consume], {"metadata.broker.list": brokers})
            process_stream(directKafkaStream,context,kafka_hosts,zookeeper_hosts,save_to_topic,es_index)
            context.start()
            context.awaitTermination()
    except:
        print(traceback.format_exc())