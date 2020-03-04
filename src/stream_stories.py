import traceback
import sys
import time
import os
import argparse
from pykafka import KafkaClient, Producer
from pprint import pprint
import csv

if __name__ == "__main__":
    def stream_data(input_file_path=None, max_stories=100):
        if input_file_path is None or input_file_path == "":
            raise Exception(f"Can not read data, input file path is empty.\n")
    
        file_exists = True
        if not (os.path.exists(input_file_path) and os.path.isfile(input_file_path)):
            file_exists = False
            raise Exception(f"Bad input file path. File: {input_file_path} does not exist\n")
        
        if not isinstance(max_stories, int) or max_stories is None:
            max_stories = 100

        return stream(input_file_path=input_file_path, max_stories=max_stories)
    
    def stream(input_file_path=None, max_stories=100):
        print(f"NOW STREAMING to Kafka from {input_file_path}")
        lines_streamed = 0
        with open(input_file_path, 'r') as readfile:
            for line in readfile:
                if max_stories > 0 and lines_streamed >= max_stories:
                    raise StopIteration
                yield line
                lines_streamed += 1
                

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('-d', '--datafile')
    parser.add_argument('-m','--maxstories')
    parser.add_argument('-t','--topic')
    parser.add_argument('-k','--kfkhosts')
    parser.add_argument('-z','--zkhosts')
    parser.add_argument('-l','--latency')
    
    args = parser.parse_args()
    produce_to_topic = None
    zookeeper_hosts = None
    kafka_hosts = None
    datafile_path = None
    max_stories = None
    latency = None
    print("Params received:\n")
    print(args)

    if not (args.datafile or isinstance(args.datafile, str)):
        raise Exception("Please provide the file path of the data to be streamed to Kafka (using the -d or --datafile flag)")
    else:
        datafile_path = args.datafile
    
    if not (args.topic or isinstance(args.topic, str)):
        raise Exception("Please provide name of kafka topic to produce stories to (using the -t or --topic flag)")
    else:
        produce_to_topic = args.topic

    if not (args.kfkhosts or isinstance(args.kfkhosts, str)):
        raise Exception("Please provide a comma separated string of Kafka hosts (using the -k or --kfkhosts flag)")
    else:
        kafka_hosts = args.kfkhosts

    if not (args.zkhosts or isinstance(args.zkhosts, str)):
        raise Exception("Please provide a comma separated string of Zookeeper hosts (using the -k or --kfkhosts flag)")
    else:
        zookeeper_hosts = args.zkhosts

    if not args.maxstories:
        raise Exception("Please provide a max number of stories to produce using the -m or --maxstories flag. Use a value of -1 to stream all available stories.")
    else:
        max_stories = args.maxstories
    try:
        max_stories = int(max_stories)
    except:
        raise Exception("Non integer value found for --maxstories")
    
    if not args.latency:
        raise Exception("Please provide latency (in secs) with which to produce messages. Default is 1 sec")
    else:
        latency = args.latency
    try:
        latency = float(latency)
    except:
        latency = 1.0

    print("Trying to establish a connection to kafka...")

    # k_client = KafkaClient(hosts='localhost:9002', zookeeper_hosts='localhost:2181')
    k_client = KafkaClient(hosts=kafka_hosts, zookeeper_hosts=zookeeper_hosts)

    print("Connection established.")
    
    print(f"Checking if topic - {produce_to_topic} exists in Kafka..")
    topics = k_client.topics
    topic_found = False
    for topic in topics:
        if hasattr(topic, 'decode'):
            topic_str = str(topic,'utf-8')
            if topic_str == produce_to_topic:
                topic_found = True
                topic_object = topic
                break
    
    if topic_found:
        print(f"Topic - {produce_to_topic} exists")
    
    if topic_found:
        print(f"Producing to topic - {produce_to_topic} [max stories to produce: {max_stories}]")
        print(f"Production latency: {latency} sec(s)")
        data_stream = stream_data(input_file_path=datafile_path,max_stories=max_stories)
        with k_client.topics[produce_to_topic].get_producer(sync=True) as producer:
            try:
                count = 0
                for line in data_stream:
                    count += 1
                    producer.produce(bytes(f"{line}", encoding='utf-8'))
                    print(f"Produced message # {count}")
                    time.sleep(latency)
                print(f"{count} Messages produced, closing producer..")
            except:
                raise
    else:
        print(f"Target Topic - {produce_to_topic} does not exist in Kafka, Exiting..")