package com.Kenny.SDE.kafka

/**
  * Created by kzhang on 4/12/17.
  */

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.utils.Utils
import org.apache.log4j.Logger

import com.Kenny.SDE.util.{WordCloudProducerConfig,URLReader, ProducerCallback}

object WordCloudProducer {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]) {

    try {
      val config = new WordCloudProducerConfig(args)

      val reader = new URLReader
      reader.init(getReaderProps(config))

      val producerProps = getNewProducerProps(config)
      val producer = new KafkaProducer[String,Int](producerProps)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          producer.close()
        }
      })

      var recordWave: Iterator[ProducerRecord[String, Int]] = null
      val sync = producerProps.getProperty("producer.type", "async").equals("sync")
      val topic = config.topic

      recordWave = reader.nextWave()
      var wave = 0
      var total = reader.getSize()
      while (true){
        var p = 0
        var progress: Float = p.toFloat/total.toFloat
        var threshold: Double = 0

        while (recordWave.hasNext) {
          try {
            var record = recordWave.next()

            var response = if (sync) producer.send(record).get() else producer.send(record,new ProducerCallback(record.topic(), record.key, record.value))
            logger.debug("Successfully sent records: " + record.toString + " with response: "+ response.toString + " Sync = " + sync.toString)
            Thread.sleep(config.sendTimeout.toLong)

          } catch {
            case e: joptsimple.OptionException =>
              logger.error("jobtsimple: " + e.getMessage)
            case e: Exception =>
              logger.error("Exception: " + e.getMessage)
            case what: Throwable => logger.error("Unkown: " + what.toString)
          }
          p += 1
          progress = p.toFloat/total.toFloat
          if (progress >= threshold)
          {
            val percentage = progress*100
            logger.info(f"Progress: $percentage%3.2f%%")
            threshold += 0.001
          }
          Thread.sleep(config.sendTimeout.toLong)
        }
        if (progress != 1)
        {
          logger.error(f"Failed to send all the message, $p%d out of $total%d message are sent. Progress: $progress%3.9f")
        }
        else {
          logger.info(f"Progress: ${progress*100}%3.2f%%")
          logger.info("I'm gonna sleep after sending the wave #" + wave + " !")
        }

        //Thread.sleep(producerProps.getProperty("sendTimeout", "2000").toLong)
        Thread.sleep(config.sendTimeout.toLong)
        recordWave = reader.nextWave()
        wave += 1
      }
    } catch {
      case e: joptsimple.OptionException =>
        logger.fatal(e.getMessage)
        System.exit(1)
      case e: Exception =>
        logger.fatal(e.printStackTrace)
        System.exit(1)
    }
    System.exit(0)
  }

  def getReaderProps(config: WordCloudProducerConfig): Properties = {
    val props = new Properties
    props.put("topic",config.topic)
    props.put("inputFile", config.inputFile)
    props.putAll(config.cmdLineProps)
    props
  }

  private def producerProps(config: WordCloudProducerConfig): Properties = {
    val props =
      if (config.options.has(config.producerConfigOpt))
        Utils.loadProps(config.options.valueOf(config.producerConfigOpt))
      else new Properties
    props.putAll(config.extraProducerProps)
    props
  }

  def getNewProducerProps(config: WordCloudProducerConfig): Properties = {

    val props = producerProps(config)

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    props.put(ProducerConfig.SEND_BUFFER_CONFIG, config.socketBuffer.toString)
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, config.retryBackoffMs.toString)
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, config.metadataExpiryMs.toString)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, config.maxBlockMs.toString)
    props.put(ProducerConfig.ACKS_CONFIG, config.requestRequiredAcks.toString)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, config.requestTimeoutMs.toString)
    props.put(ProducerConfig.RETRIES_CONFIG, config.messageSendMaxRetries.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.sendTimeout.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.maxMemoryBytes.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.maxPartitionMemoryBytes.toString)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "wordCloudProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("security.protocol", config.securityProtocol.toString)

    props
  }

}
