# Word Cloud

## Environment

- 4 Nodes Hadoop Cluster running with
    - Scala 2.11
    - HDP 2.5.3.0

## Dependencies

- Spark 2.0
- Kafka 0.10
- Hive 1.2
- Zeppelin 0.6
- SBT

## Design/Tradeoffs

This solution are divided into the following.

### Kafka Producer
- Kafka producer is to efficiently simulate URL stream coming from upstream and store the URLs safely in a distributed replicated clsuter.
- Create a compacted kafka topic, URL is produced as the Key of ProducerRecord, so no duplicated URL will be stored in Kafka cluster.
- The topic can be created with multiple partitions and multiple replica for scalability purpose.
- The ProducerRecord can be send in either sync or async with callback function to ensure delivery.

### Spark Streaming
- Spark stateful streaming job to consume the URLs from Kafka cluster and process the URLs to extract the top common words from those web pages.
- Use dynamic resource allocation and direct approach for easy accommodate when kafka cluster scale up.
- Use kafka again to commit and store message offset to ensure message processed exactly once.
- Use checkpoint to ensure streaming application can be recovered from failure.
- Use spark bloom filter to ensure no duplicated URL processed, also provide feedback to master bloom filter to ensure only filter the valid URLs with product description available after being processed. 
- Use optional switch to store the whole word cloud or just the most common words (topK) or both in hive.
- Use Count-min sketch to estimate the top Kth words.
- Save result to Hive table(s) for future use.
- Expose streaming, analysis parameters in wordCloud-defaults.conf. Please refer to `conf/` folder for reference. 

### Zeppelin visualization
- Use Zeppelin notebook to query and visualize the word cloud.

## How to

### Environment setup
- Deploy a HDP cluster with Spark, Hive, Kafka, Zookeeper, Zeppelin services installed
- Create a Kafka topic (Optionally as compacted topic)
    ```
    <Kafka_home>/bin/kafka-topics.sh --create --zookeeper <ZK_Quorum> --replication-factor 2 --partitions 2 --topic wordCloud --config cleanup.policy=compact
    ```
- Create two hive tables with following schema:  
    ```
    hive> CREATE TABLE `wordcloud`(
   `words` string,
   `count` bigint) STORED AS PARQUET;
   ```
   ```
   hive> CREATE TABLE `topkwords`(
     `word` string) STORED AS PARQUET;
   ```
- Setup Zeppelin to use JDBC interpreter to query and visualize above hive data.

- After compile the Uber jar, run the following:
    - To start the kafka producer
    ```
    <JAVA_HOME>/bin/java -Dlog4j.configuration=file:./conf/log4j.properties -Dkafka.logs.dir=<log_dir> -Xmx512M -server -cp ./wordcloud.jar com.Kenny.SDE.kafka.WordCloudProducer --broker-list <broker_list> --topic wordCloud --input-file ../kafka/url.txt --timeout 1000 &
    ```
    - To start the spark streaming
    ```
    spark-submit --master yarn-cluster --files conf/wordCloud-defaults.conf,/usr/hdp/current/spark2-client/conf/hive-site.xml --properties-file conf/spark.properties --class com.Kenny.SDE.spark.WordCloud wordcloud.jar --batch 120 --switch 2 --myProperties ./wordCloud-defaults.conf --checkpointPath /tmp/checkpoint
    ```

