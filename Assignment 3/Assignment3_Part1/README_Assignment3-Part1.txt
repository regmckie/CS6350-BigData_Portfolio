FILENAME: README_Assignment3-Part1.txt (this README is for Assignment 3, Part 1 - Spark Streaming with Real Time Data and Kafka)
DUE DATE: 7/28/2024
AUTHOR:   Reg Gonzalez
EMAIL:    rdg170330@utdallas.edu (school) or regmckie@gmail.com (personal)
COURSE:   CS 6350.0U1, Summer 2024


DESCRIPTION:
"In this part, you will create a Spark Streaming application that will continuously read text data from
a real time source, analyze the text for named entities, and send their counts to Apache Kafka. A
pipeline using Elasticsearch and Kibana will read the data from Kafka and analyze it visually."


HOW TO COMPILE AND RUN:
I developed the 'read_in_subreddit.py' and 'wordcount_named_entities.py' files in Notepad++, but
ran and tested them using WSL in Windows Powershell. To run this part of the assignment, I'm going to assume
you've installed WSL in your Windows machine, if you're using a Windows machine. You also need to install Apache Spark
and Kafka using Quickstart—for the latter, we'll be using Kafka with Zookeeper. 

I'm running the following steps in Windows Powershell (in Administrator mode):

{STEP 1}: Start Zookeeper server
.\wsl
cd ~
cd kafka_2.13-3.7.1
bin/zookeeper-server-start.sh config/zookeeper.properties

{STEP 2}: In another window, start Kafka server
.\wsl
cd ~
cd kafka_2.13-3.7.1
bin/kafka-server-start.sh config/server.properties

{STEP 3}: In another window, create topic1 and topic2
.\wsl
cd ~
cd kafka_2.13-3.7.1
bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092

{STEP 4}: In the same or another window, run 'wordcount_named_entities.py'
.\wsl
cd ~
cd kafka_2.13-3.7.1	// This is the directory I saved the 'wordcount_named_entities.py' file
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 wordcount_named_entities.py localhost:9092 subscribe topic1

Note: It might take a minute or two for it to start running. Be patient and begin STEP 5 *after* you see the following on-screen:
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+
|entity|count|
+------+-----+
+------+-----+

{STEP 5}: In another window, run 'read_in_subreddit.py'
.\wsl
cd ~
cd kafka_2.13-3.7.1 	// This is the directory I saved the 'read_in_subreddit.py' file
python3 read_in_subreddit.py

{STEP 6}: Terminate programs
Both programs will run for a minute or two and will eventually stop. However, if you want to terminate them,
do Ctrl + C—first on 'read_in_subreddit.py' and then on 'wordcount_named_entities.py'

{STEP 7}: Check topic1 and topic2 outputs to verify
bin/kafka-console-consumer.sh --topic topic1 --from-beginning --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic topic2 --from-beginning --bootstrap-server localhost:9092 

There is a report that includes screenshots of the outputs of topic1 and topic2 and provides
summaries about the results of the code. The report is called "Assignment 3, Part 1 - Report."


DATASET USED:
https://www.reddit.com/r/rupaulsdragrace/

This is the subreddit I'm reading in for this part of Assignment 3.