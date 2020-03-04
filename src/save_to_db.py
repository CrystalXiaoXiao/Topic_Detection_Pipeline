import mysql.connector
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import ConsumerStoppedException, SocketDisconnectedError, LeaderNotAvailable
import argparse
from getpass import getpass
import time
import sys
import traceback
import ujson

if __name__ == "__main__":
	parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
	parser.add_argument('-d','--dbhost')
	parser.add_argument('-p','--dbport')
	parser.add_argument('-u','--dbuser')
	parser.add_argument('-s','--dbpass')
	parser.add_argument('-n','--nodbpass')
	parser.add_argument('-t','--topic')
	parser.add_argument('-e','--dbname')
	parser.add_argument('-k','--kfkhosts')
	parser.add_argument('-z','--zkhosts')

	args = parser.parse_args()
	db_host = 'localhost'
	db_port = 3306
	db_user = 'unknown'
	bypass_db_pass = False
	topic_to_consume = None
	db_name = None
	password = None
	zookeeper_hosts = None
	kafka_hosts = None
	
	if not (args.dbhost or isinstance(args.dbhost, str)):
		raise Exception("Please provide name of mysql db host to connect to or use an empty string to use 'localhost' (using the -d or --dbhost flag)")
	else:
		if not args.dbhost in ['',' ']:
			db_host = args.dbhost
	
	if not (args.dbport or isinstance(args.dbport, str)):
		raise Exception("Please provide name of mysql db port to connect to or use an empty string to use the default port 3306 (using the -p or --dbport flag)")
	else:
		if not str(args.dbport) in ['',' ']:
			db_port = args.dbport

	if not (args.dbuser or isinstance(args.dbuser, str)):
		raise Exception("Please provide name of mysql db user to connect mysql with (using the -u or --dbuser flag)")
	else:
		db_user = args.dbuser

	if not (args.dbname or isinstance(args.dbname, str)):
		raise Exception("Please provide name of mysql db name to connect to (using the -e or --dbname flag)")
	else:
		db_name = args.dbname

	if not (args.topic or isinstance(args.topic, str)):
		raise Exception("Please provide name of kafka topic to consume date from (using the -t or --topic flag)")
	else:
		topic_to_consume = args.topic
	
	if not (args.kfkhosts or isinstance(args.kfkhosts, str)):
		raise Exception("Please provide a comma separated string of Kafka hosts (using the -k or --kfkhosts flag)")
	else:
		kafka_hosts = args.kfkhosts

	if not (args.zkhosts or isinstance(args.zkhosts, str)):
		raise Exception("Please provide a comma separated string of Zookeeper hosts (using the -k or --kfkhosts flag)")
	else:
		zookeeper_hosts = args.zkhosts

	print(args)
	if args.nodbpass and args.nodbpass == 'yes':
		bypass_db_pass = True
	else:
		password = getpass()
    
	mysql_con = None
	try:
		if bypass_db_pass:
			mysql_con = mysql.connector.connect(user=db_user, host=db_host, database=db_name)
		else:
			mysql_con = mysql.connector.connect(user=db_user, password=password, host=db_host, database=db_name)
		print(f"Successfully connected to mysql database {db_name} with user:{db_user} at mysql://{db_host}:{db_port}")
	except:
		print(f"Error connecting to mysql database with user:{db_user} at mysql://{db_host}:{db_port}")
		sys.exit(-1)
	
	k_client = None
	print("Trying to establish a connection to kafka...")
    
	try:
		k_client = KafkaClient(zookeeper_hosts=zookeeper_hosts)
		print("Connection established.")
	except:
		print("Connection could not be established to Kafka.")
		sys.exit(-1)
	
	kafka_topic = None
	try:
		topic_exists = False
		consume_n_messages = 11
		for topic in k_client.topics:
			if str(topic,'utf-8') == topic_to_consume:
				kafka_topic = k_client.topics[topic]
				topic_exists = True
				break
		if not topic_exists:
			raise Exception(f"Topic - {topic_to_consume} - does not exist in Kafka")
	except:
		print(traceback.format_exc())
		sys.exit(-1)
	
	# starts from the latest available offset
	consumer = kafka_topic.get_simple_consumer(
		consumer_group="save_to_db_group",
		auto_commit_enable=True,
		auto_offset_reset=OffsetType.LATEST
	)

	try:
		msg_cnt = 0
		cursor = mysql_con.cursor()
		for msg in consumer:
			story = dict()
			msg_cnt += 1
			print(f"Consuming Message # {msg_cnt}")
			try:
				str_msg = str(msg.value, encoding='utf-8')
			except:
				str_msg = msg.value
			try:
				story = ujson.loads(str_msg)
				print(f"Message is valid dict")
				story_guid = story.get('story_guid', None)
				title = story.get('title',None)
				text = story.get('text',None)
				pub_feed_url = story.get('pub_feed_url', None)
				dest_url = story.get('destination_url', None)
				tags_json = ujson.dumps({'tags': story.get('tags',[]).split('|TAG|')}) if story.get('tags',None) else None
				category = story.get('category', None)
				p_img_url = story.get('primary_image_url', None)
				p_img_width = story.get('primary_image_width', None)
				p_img_height = story.get('primary_image_height', None)
				s_img_url = story.get('secondary_image_url', None)
				s_img_width = story.get('secondary_image_width', None)
				s_img_height = story.get('secondary_image_height', None)
				publisher = story.get('publisher', None)
				pub_dom = story.get('publisher_domain', None)
				topics_dict = story.get('topics', {})
				topics = ",".join(topics_dict.keys())
				pubDate = story.get('pub_date_formatted', None)
				modDate = story.get('mod_date_formatted', None)
				parsedDate = story.get('parsed_date_formatted', None)
				
				# Insert topic names in the topics table and fetch its ids
				
				topics_db = {}
				if title == "Exclusive: Quikr Raises $2.9 Mn Debt From Trifecta Capital":
					print(topics_dict)
					
				for tp in topics_dict:
					topics_check_query = ("SELECT topic_id from topics T where T.topic_name = %s")
					cursor.execute(topics_check_query, [tp])
					topic_present = False
					for (topic_id,) in cursor:
						topic_present = True
						print(f"Topic {tp} already present in DB with id : {topic_id}")
						topics_db[tp] = { 'score': topics_dict[tp], 'id': topic_id }
					if topic_present:
						continue
					topics_insert_query = ("INSERT IGNORE INTO topics (topic_name)\
								VALUES (%s)")
					cursor.execute(topics_insert_query, [tp])
					tp_id = cursor.lastrowid
					if tp_id and tp_id != 0:
						topics_db[tp] = { 'score': topics_dict[tp], 'id': cursor.lastrowid }
					
					# else:
					# 	print(f"Duplicate topic encountered {tp}")
				
				# Insert story in the stories table and fetch its id
				print("Inserting story in the stories table and fetching its id")
				text = text[:1000]
				stories_row_data = (text, story_guid, title, pub_feed_url, dest_url, tags_json, category, p_img_url, p_img_width, p_img_height,
							s_img_url, s_img_width, s_img_height, publisher, pub_dom, pubDate, modDate, parsedDate )
				stories_insert_query = ("INSERT IGNORE INTO stories (excerpt, guid, title, feed_url, dest_url, tags, category, prm_image_url, p_image_width, p_image_height, \
        						sec_image_url, s_image_width, s_image_height, publisher, publisher_domain, pub_date, mod_date, parsed_date) \
								VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
				
				
				cursor.execute(stories_insert_query, stories_row_data)
				story_id = cursor.lastrowid
				print(f"Story id is: {story_id}")
				if title == "Exclusive: Quikr Raises $2.9 Mn Debt From Trifecta Capital":
					print(f"story_id: {story_id}. If not 0 and not None, will insert topics")

				if story_id and story_id != 0:
					print(f"Inserting in bridge table")
					# btb is the stories_topics bridge table
					for tname, tvals in topics_db.items():
						# btb_insert_query = ("INSERT IGNORE INTO stories_topics VALUES(%s, %s, %s)")
						btb_insert_query = ("INSERT IGNORE INTO stories_topics VALUES(%s, %s, %s)")
						btb_data = (story_id, tvals.get('id',None), tvals.get('score', 0.0))
						if title == "Exclusive: Quikr Raises $2.9 Mn Debt From Trifecta Capital":
							print(f"btb_data: {btb_data}")
						cursor.execute(btb_insert_query, btb_data)
				# else:
				# 	print("Duplicate story encountered")
				
				mysql_con.commit()

				print(f"Story {story_guid} processed")
			except mysql.connector.Error as err:
				print(err)
				print(traceback.format_exc())
				continue
			except:
				print(traceback.format_exc())
				break
				# continue
			time.sleep(1)
	except (SocketDisconnectedError) as e:
		consumer = kafka_topic.get_simple_consumer()
		# use either the above method or the following:
		consumer.stop()
		consumer.start()
	except:
		print(traceback.format_exc())
		sys.exit(-1)
	finally:
		if cursor:
			cursor.close()
		if mysql_con:
			mysql_con.close()
	
	if mysql_con:
		mysql_con.close()
		input("Press any key to exit")
		
							


