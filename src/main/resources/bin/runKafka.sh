#!/usr/bin/env bash

#! create the topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper s1.poc.support.com:2181,s2.poc.support.com:2181,s3.poc.support.com:2181 --replication-factor 2 --partitions 2 --topic wordCloud

#! desc the topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper s1.poc.support.com:2181,s2.poc.support.com:2181,s3.poc.support.com:2181 --describe --topic wordCloud

#! view the topic
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper s1.poc.support.com:2181,s2.poc.support.com:2181,s3.poc.support.com:2181 --topic wordCloud --property print.key=true --property print.timestamp=true --from-beginning

#! start the producer
/usr/jdk64/jdk1.8.0_77/bin/java -Dlog4j.configuration=file:./conf/log4j.properties -Dkafka.logs.dir=/home/spark/wordCloud/spark/log -Xmx512M -server -cp ./wordcloud.jar com.Kenny.SDE.kafka.WordCloudProducer --broker-list S3.poc.support.com:6667,S4.poc.support.com:6667 --topic wordCloud --input-file ../kafka/url.txt --timeout 1000 &