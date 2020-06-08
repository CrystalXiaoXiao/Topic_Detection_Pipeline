# Topic_Detection_Pipeline
The aim of this project was to build a near real-time topic detection pipeline for the news stories published by news sources on the web. If stories are tagged with topics, it becomes easier for the reader to discover them and to follow topics that he/she may be interested in.

Architecture
![Topic Detection Data Pipeline](/bTopicDetectionPipelineArchitecture.png)

A distributed data processing pipeline consists of many moving parts which have to work in unison to make the whole think work. If one of the components fail to work as expected at any time, it creates a ripple effect downstream preventing any further processing. Moreover, debugging a distributed application is not easy. That is why, the architecture and design of the pipeline needs to be carefully thought out. One of those design considerations is choosing the right set of software tools to work with. Fortunately today, there is a lot of great choices available for the job and it helps that many of them are open-source and stable enough to be used by the big technology firms to process terabytes of data on a usual basis.

 The aim of this project was to build a near real-time topic detection pipeline for the news stories published by news sources on the web. If stories are tagged with topics, it becomes easier for the reader to discover them and to follow topics that he/she may be interested in.

Check out the demo [here](http://www.navitgrover.xyz/topics-detection) and the code at github
__Note: Demo is hosted on fre tier, so it may not be available right now__

 The tools and software frameworks used to build this pipeline are listed below. It will be followed by Using each.

* Scrapy Framework: To create a ditributed parser to parse stories from rss feeds
* A __Redis__ Server: To hold data for coordination between parser workers, scheduling and other parser related metadata
* An __Elasticsearch__: To put word to topic mapping.
* __Kafka__: To hold the data in motion
* __Spark (Streaming)__: To process the data stream in a distributed setting
* __AWS__ Resources
    * AWS __Lambda__: To query the database without using a web server
    * AWS __Cognito__: To access data in Mysql publically without exposing credentials
    * AWS __RDS__ (Mysql): To store processed stories

__Using Scrapy__

I created a distributed parser to parse stories from the RSS feeds of various news websites using the scrapy framework in python. The parser code runs on multiple machines independently and co-ordination between them is carried out using a central redis server which hosts the configuration required for coordination and other metadata related to parsing to help with deduplication, work load balancing and scheduling. The parser code is responsible for extracting text, title and other metadata from the rss feeds.

__Using Elasticsearch__

In order to infer a topic for a story, a searchable dataset is required that maps important words in the story to topics. Using an interactive tool that I built that allows a researcher to extract clusters of related words from a text corpus, I created such dataset and put it into an elasticsearch index named “topics” so that the dataset is searchable over HTTP. 

__Using Kafka__

To process the scraped stories in real-time, a temporary data storage was needed to host the data in-motion and purge it when it was not needed or saved to a permanent archive. I chose Apache Kafka for the purpose as it offers the robustness and data processing guarantees while providing a simple api to work with. Data records are put into topics that are sharded across machines which provides both - high availability and a fast response time. Raw stories are put into a topic named “stories” and were available to be queried for downstream tasks. 

__Using Spark__

 Kafka does not provide any data processing capabilities on its own which is why a distributed real-time stream processing framework was needed to extract map stories to topics. I chose Apache Spark for the job as it offers an interface for processing streams of data across multiple machines. Using the Spark application was to ingest the stories from Kafka topic, processes the stories, map them to topics, rank the topics for each story and put the processed stories back into a different Kafka topic named, for the purposes of this demo “stories2”.

Spark streaming uses a concept called DStreams which are micro batches of data that simulates a real-time data stream. The DStreams are passed to the executors (java worker resources) for processing. The executors are created dynamically for each DStream afresh on different machines. The executor tasks are short-lived i.e. once the chunk of records (data partition) in the DStream for which it is responsible for are processed, they are killed. This means that if the executors need to create connections to other resources, they need to be created for each DStream. In our case, we needed the executors to create connection to Elasticsearch (for pulling word-topic mapping information) and to Kafka (for parking the processed stories) Elasticsearch connections are unfortunately not serialisable and thus have to be created for each DStream partition but at least they can be shared for records within a partition.

Another python script creates a Kafka consumer for the “stories2” topic which pulls the stories from the topic and puts them into a permanent storage for the purpose of being queried. 


__AWS__
    
 Once the stories are available in the database they can be queried through a REST API. However, due to the lack of resources to host the API server on the cloud, I chose to employ a serveless strategy to query data from the database on an ad-hoc basis. I used AWS Lambda functions to hold the logic to pull stories from the database and pass them back to a user’s browser.

AWS resources are not exposed without authentication and since the demo was to be exposed to a publicly accessible website, I needed a way to explicitly grant permission to access the database but without sharing the aws credentials in the code to be run on the client. AWS provides an authentication framework called Cognito through which an Identity Id can be created and necessary permissions/roles can be associated with the identity so that AWS can determine mapping from an identity Id to the resources it can access.     
