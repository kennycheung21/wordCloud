zip -d wordcloud.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

export SPARK_MAJOR_VERSION=2
spark-submit --master yarn-cluster --num-executors 2 --files conf/wordCloud-defaults.conf,/usr/hdp/current/spark2-client/conf/hive-site.xml --properties-file conf/spark.properties --class com.Kenny.SDE.spark.WordCloud wordcloud.jar --batch 120 --switch 2 --myProperties ./wordCloud-defaults.conf --checkpointPath /tmp/checkpoint