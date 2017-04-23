package com.Kenny.SDE.spark

import java.io._

import scopt.OptionParser

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkException}
import org.apache.spark.sql.{SparkSession}

import org.apache.spark.util.sketch.BloomFilter

import com.twitter.algebird._
import com.twitter.algebird.CMSHasherImplicits._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.Map
import org.apache.log4j.LogManager

import com.Kenny.SDE.util._

/**
  * Created by kzhang on 4/20/17.
  */
object WordCloud {
  private val logger = LogManager.getLogger(getClass)

  val DEFAULT_PROPERTIES_FILE = "conf/wordCloud-defaults.conf"

  def main(args: Array[String]) {

    val defaultParams = WordCloudParams()

    val parser = new OptionParser[WordCloudParams]("WordCloud") {
      head("Streaming Word Cloud: a streaming app for Amazon product description word count.")
      opt[String]('b', "batch")
        .required()
        .text("the interval of each batch for spark streaming")
        .action((x, c) => c.copy(batch = x.toInt))
      opt[String]('s', "switch")
        .required()
        .text("0: traditional word count, 1: use CMS, 2: use Both")
        .action((x, c) => c.copy(switch = x.toInt))
      opt[String]('p', "myProperties")
        .text("the config properties file location for wordCloud")
        .action((x, c) => c.copy(propertiesFile = x))
      opt[String]('c', "checkpointPath")
        .text("the checkpoint path for spark streaming")
        .action((x, c) => c.copy(checkpointDir = x))
      note(
        """
          |Usage: wordCloud.jar --batch 120 --switch 2 --myProperties conf/wordCloud-defaults.conf --checkpointPath /tmp/checkpoint
          |  <batch> the interval of each batch for spark streaming, 60 by default. [required]
          |  <switch> the switch to determine which method to use for word count:- 0: traditional word count, 1: use CMS, 2: use Both (default) [required]
          |  [cpDir] the checkpoint path for spark streaming, /tmp/checkpoint by default. [optional]
          |  [myProperties] the config properties file for streamingKMeans, conf/wordCloud-defaults.conf by default. [optional]
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map {
      p => {
        println("Starting to run the streaming word cloud with parameters: \n" + p.toString)
        run(p)
      }
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: WordCloudParams){

    val batch = params.batch
    val switch = params.switch
    val propertiesFile: String = params.propertiesFile
    val checkpointPath : String = params.checkpointDir

    //Process and prepare the parameters
    val wholeConfigs = loadPropertiesFile(propertiesFile)
    val kafkaParams = prepareKafkaConfigs(wholeConfigs, batch.toInt)
    val algParams = prepareAlgConfigs(wholeConfigs)

    //Bloom Filter parameters
    val wordFilter: Array[String] = algParams.getOrDefault("bf.filter", "amazon").split(",")
    val FPP: Double = algParams.getOrDefault("bf.fpp", "0.03").toDouble
    val EXPNUM: Long = algParams.getOrDefault("bf.expnum", "1000").toLong

    //CMS parameters
    val topK: Int = algParams.getOrDefault("cms.topk", "10").toInt
    val DELTA: Double = algParams.getOrDefault("cms.delta", "1E-3").toDouble
    val EPS: Double = algParams.getOrDefault("cms.eps", "0.01").toDouble
    val SEED: Int = algParams.getOrDefault("cms.seed", "1").toInt
    val PERC: Double = algParams.getOrDefault("cms.perc", "0.001").toDouble

    if (wholeConfigs.getOrDefault("verbose", "false") == "true" )
    {
      println(s"Logger Class: ${logger.getClass.getName}. Logger lever: ${logger.getEffectiveLevel}")
      println(s"wholeConfigs: ${wholeConfigs.toString()}")
      println(s"kafkaParams: ${kafkaParams.toString()}")
      println(s"algParams: ${algParams.toString()}")
    }

    val topics = kafkaParams.getOrDefault("topics", "wordCloud").toString

    val spark = SparkSession.builder().appName("KennyWordCloud").enableHiveSupport().getOrCreate()

    val bWordFilter = spark.sparkContext.broadcast(wordFilter)

    import spark.implicits._

    val cb = new SparkCallback()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(batch.toLong))
    ssc.checkpoint(checkpointPath)

    val initialRDD = ssc.sparkContext.parallelize(List(("amazon", 1L)))

    val initialFilter = spark.createDataset(List("http://www.amazon.com/"))

    val masterBF : BloomFilter = initialFilter.stat.bloomFilter("value", EXPNUM, FPP)
    logger.info(s"Initial bloom filter bit size: ${masterBF.bitSize()}")
    val topicsSet = topics.split(",").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, Int](ssc, PreferConsistent, Subscribe[String,Int](topicsSet, kafkaParams))

    //consumerRecords => URLs
    val urlStream = kafkaStream.transform { rdd =>
      var offsetRanges = Array[OffsetRange]()
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      logger.info("OffsetRanges of RDD " + rdd.id + " : " + offsetRanges.mkString(";"))

      val urls = rdd.map(_.key())
      val numUrls = urls.count()
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges, cb)
      logger.info(s"Batch begins. Total number of URL is ${numUrls}")
      urls
    }

    //Transform ONLY
    val descPairStream = urlStream.transform{rdd =>

      val newURL = rdd.distinct().filter(u => !masterBF.mightContain(u))
    //extra top word
    val descPair = newURL.map{ url =>
      val amazonProduct = AmazonProduct(url)
      val urlDesc = amazonProduct.extractTopWord(bWordFilter.value)
      urlDesc
    }.filter(_.isDefined).collect{
      case Some(result) => {
        logger.info(s"extract the top words: ${result} as rdd ${rdd.id}")
        result
      }
    }

      descPair
    }

    descPairStream.cache() //save before the switch for future reuse

    //traditional word count
    if (switch%2 == 0){
      val wordCountStream = descPairStream.map(_._2).flatMap(_.split(" ")).map{ a:String => (a, 1)}

      def mappingFunc (word: String, count: Option[Int], state: State[Long]):Option[(String, Long)] = {
        val sum = count.getOrElse(0).toLong + state.getOption.getOrElse(0L)
        val output = (word, sum)
        state.update(sum)
        Some(output)
      }

      val stateDStream = wordCountStream.mapWithState(StateSpec.function(mappingFunc _).initialState(initialRDD)) //optinally set the timeout for spark to forget the idle words after some time

      val stateSnapshotStream = stateDStream.stateSnapshots()

      val topWordState = stateSnapshotStream.transform{rdd =>
        val topWordCount = rdd.sortBy(_._2, false)
        topWordCount
      }

      topWordState.print(topK) //13

      topWordState.foreachRDD{ rdd =>
        val topWordCountDF = rdd.toDF("Words", "Count")
        //topWordCountDF.printSchema()
        topWordCountDF.write.mode("overwrite").saveAsTable("wordCloud")
      }

      descPairStream.map(_._1).foreachRDD{ validURL =>
        val count = validURL.count
        val tempBF = validURL.toDS().stat.bloomFilter("value", 1000, 0.03)
        masterBF.mergeInPlace(tempBF)
        logger.info(s"After Merged bloom filter, with ${count} valid url.")
      }
    }


    //TopK words
    if (switch > 0){
      implicit val monoid = TopPctCMS.monoid[String](eps = EPS, delta = DELTA, seed = SEED, heavyHittersPct = PERC)

      val initialDesc = Seq("amazon")

      val initialCMSRDD = ssc.sparkContext.parallelize(List(("wordCloud", monoid.create(initialDesc))))

      val CMSMapppingFunc = (key: String, newData: Option[TopCMS[String]], state: State[TopCMS[String]]) => {
        val sum = state.get() ++ newData.getOrElse(monoid.create("amazon"))
        val output = (key, sum)
        state.update(sum)
        output
      }

      val monoidStream = descPairStream.map(_._2).map(d => d.split(" ").toSeq).map{s => ("wordCloud", monoid.create(s))}

      val stateStream = monoidStream.mapWithState(StateSpec.function(CMSMapppingFunc).initialState(initialCMSRDD))

      stateStream.stateSnapshots.foreachRDD{ rdd =>
        val size = rdd.count()

        if (size > 0) {
          val m = rdd.first()._2

          val tk = m.heavyHitters.map{ id => (id, m.frequency(id).estimate)}.toList.sortBy { case (_, count) => -count }.slice(0, topK)
          logger.info(s"Batch end with TopK elements: \n${tk.mkString("\n")}")

          val df = rdd.map(_._2).flatMap{ m =>
            val tk = m.heavyHitters.map{ id => (id, m.frequency(id).estimate)}.toList.sortBy { case (_, count) => -count }.slice(0, topK)
            tk
          }.map(_._1).toDF("word")

          df.write.mode("overwrite").saveAsTable("topKWords")
        }
        logger.info(s"Batch really End with no new top-words.")
      }
    }

    ssc.remember(Minutes(3))
    ssc.start()
    ssc.awaitTermination()

  }

  @throws[IOException]
  def loadPropertiesFile (propertiesFile: String) : Map[String, String] = {
    val props = new Properties
    var map :Map[String, String] = Map()
    var propsFile: File = null
    if (propertiesFile != null) {
      propsFile = new File(propertiesFile)
      if (!propsFile.isFile)
      {
        throw new FileNotFoundException(s"Invalid properties file ${propertiesFile}.")
      }
    }
    else propsFile = new File(DEFAULT_PROPERTIES_FILE)

    if (propsFile.isFile) {
      var fd:FileInputStream = null
      try {
        fd = new FileInputStream(propsFile)
        props.load(new InputStreamReader(fd, "UTF-8"))
        map = props.stringPropertyNames()
          .map(k => (k, props(k).trim))
          .toMap
      } finally {
        if (fd != null) {
          try {
            fd.close()
          }catch {
            case e: IOException => {
              throw new SparkException(s"Failed when loading Spark properties from ${propsFile.getName}", e)
            }
          }
        }
      }
    } else {
      throw new FileNotFoundException(s"Default properties file ${DEFAULT_PROPERTIES_FILE} not found.")
    }
    map
  }

  def prepareKafkaConfigs (wholeConfig: Map[String, String], batch: Int = 3, prefix: String = "kafkaStream."): Map[String, Object] = {
    var kafkaParam = new ConcurrentHashMap[String, Object]
    val DEFAULT_BOOTSTRAP_SERVER = "localhost:6667"
    val DEFAULT_TOPICS = "test"
    val DEFAULT_GROUP_ID = "KMean_Consumer"
    val DEFAULT_KEY_DESERIALIZER = classOf[IntegerDeserializer]
    val DEFAULT_VALUE_DESERIALIZER = classOf[StringDeserializer]
    val MIN_HEARTBEAT_MS = (batch + 5) * 1000 // add the some cushions
    val MIN_SESSION_TIMEOUT_MS = MIN_HEARTBEAT_MS * 3

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix)) kafkaParam.put(k.substring(prefix.length), v)
    }

    logger.debug(s"Before injecting the default values, kafkaParams: ${kafkaParam.toString}")

    if (!kafkaParam.containsKey("bootstrap.servers")){
      logger.info(s"Property bootstrap.servers is not found, setting it to default value ${DEFAULT_BOOTSTRAP_SERVER}.")
      kafkaParam.put("bootstrap.servers", DEFAULT_BOOTSTRAP_SERVER)
    }
    if (!kafkaParam.containsKey("topics")){
      logger.info(s"Property topics is not found, setting it to default value ${DEFAULT_TOPICS}.")
      kafkaParam.put("topics", DEFAULT_TOPICS)
    }
    if (!kafkaParam.containsKey("group.id")){
      logger.info(s"Property group.id is not found, setting it to default value ${DEFAULT_GROUP_ID}.")
      kafkaParam.put("group.id", DEFAULT_GROUP_ID)
    }
    if (!kafkaParam.containsKey("key.deserializer")){
      logger.info(s"Property key.deserializer is not found, setting it to default value ${DEFAULT_KEY_DESERIALIZER.toString}.")
      kafkaParam.put("key.deserializer", DEFAULT_KEY_DESERIALIZER)
    }
    if (!kafkaParam.containsKey("value.deserializer")){
      logger.info(s"Property value.deserializer is not found, setting it to default value ${DEFAULT_VALUE_DESERIALIZER.toString}.")
      kafkaParam.put("value.deserializer", DEFAULT_VALUE_DESERIALIZER)
    }


    if (!kafkaParam.containsKey("heartbeat.interval.ms")) {
      logger.info(s"Property heartbeat.interval.ms is not found, setting it to batch value ${MIN_HEARTBEAT_MS}.")
      kafkaParam.put("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString)
    }else {
      if (kafkaParam.getOrDefault("heartbeat.interval.ms", "3000").asInstanceOf[Int] < MIN_HEARTBEAT_MS){
        logger.info(s"Property heartbeat.interval.ms is less than the batch interval, setting it to batch value ${MIN_HEARTBEAT_MS}")
        kafkaParam.update("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString)
      }
    }

    val hearbeat = kafkaParam.getOrDefault("heartbeat.interval.ms", MIN_HEARTBEAT_MS.toString).toString.toInt
    var sessionTimeout = kafkaParam.getOrDefault("session.timeout.ms", "10000").toString.toInt

    if (sessionTimeout < hearbeat * 3){
      logger.info(s"Property session.timeout.ms is less than the 3 times of batch interval, setting it to batch value ${hearbeat*3}")
      kafkaParam.update("session.timeout.ms", (hearbeat*3).toString)
      sessionTimeout = hearbeat*3
    }

    val requestTimeout = kafkaParam.getOrDefault("request.timeout.ms", "40000").toString.toInt

    if (requestTimeout <= sessionTimeout){
      logger.info(s"request.timeout.ms is less than session.timeout.ms, setting it to ${sessionTimeout+10000}")
      kafkaParam.update("request.timeout.ms", (sessionTimeout+10000).toString)
    }

    kafkaParam.toMap[String,Object]
  }

  def prepareAlgConfigs (wholeConfig: Map[String, String], prefix: String = "alg."): Map[String, String] = {

    var algParams = new ConcurrentHashMap[String, String]

    //Bloom Filter params
    val DEFAULT_WORDFILTER = Array("the", "is", "are", "and", "this", "that", "a", "&", "for", "to", "in", "with", "your" )
    val DEFAULT_FPP = 0.03
    val DEFAULT_EXPNUM = 1000

    //CMS
    val DEFAULT_TOPK = 10
    val DEFAULT_DELTA = 1E-3
    val DEFAULT_EPS = 0.01
    val DEFAULT_SEED = 1
    val DEFAULT_PERC = 0.001

    wholeConfig.foreach{ case (k, v) =>
      if (k.startsWith(prefix))
        algParams.put(k.substring(prefix.length), v)
    }

    if (!algParams.containsKey("bf.filter")){
      logger.info(s"Property topK is not found, setting it to default value ${DEFAULT_WORDFILTER.mkString(",")}.")
      algParams.put("bf.filter", DEFAULT_WORDFILTER.mkString(","))
    }
    if (!algParams.containsKey("bf.fpp")){
      logger.info(s"Property topK is not found, setting it to default value ${DEFAULT_FPP}.")
      algParams.put("bf.fpp", DEFAULT_FPP.toString)
    }
    if (!algParams.containsKey("bf.expnum")){
      logger.info(s"Property topK is not found, setting it to default value ${DEFAULT_EXPNUM}.")
      algParams.put("bf.expnum", DEFAULT_EXPNUM.toString)
    }

    if (!algParams.containsKey("cms.topk")){
      logger.info(s"Property topK is not found, setting it to default value ${DEFAULT_TOPK}.")
      algParams.put("cms.topk", DEFAULT_TOPK.toString)
    }
    if (!algParams.containsKey("cms.delta")){
      logger.info(s"Property DecayFactor is not found, setting it to default value ${DEFAULT_DELTA}.")
      algParams.put("cms.delta", DEFAULT_DELTA.toString)
    }
    if (!algParams.containsKey("cms.eps")){
      logger.info(s"Property RandomCenters.dim is not found, setting it to default value ${DEFAULT_EPS}.")
      algParams.put("cms.eps", DEFAULT_EPS.toString)
    }
    if (!algParams.containsKey("cms.seed")){
      logger.info(s"Property RandomCenters.weight is not found, setting it to default value ${DEFAULT_SEED}.")
      algParams.put("cms.seed", DEFAULT_SEED.toString)
    }
    if (!algParams.containsKey("cms.perc")){
      logger.info(s"Property RandomCenters.weight is not found, setting it to default value ${DEFAULT_PERC}.")
      algParams.put("cms.perc", DEFAULT_PERC.toString)
    }
    algParams.toMap[String, String]
  }
}
