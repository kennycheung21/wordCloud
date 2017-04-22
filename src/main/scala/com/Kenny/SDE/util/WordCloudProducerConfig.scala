package com.Kenny.SDE.util

import joptsimple.OptionParser
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import kafka.serializer.DefaultEncoder
import kafka.utils.{CommandLineUtils, ToolsUtils}

/**
  * Created by kzhang on 4/21/17.
  */
class WordCloudProducerConfig(args: Array[String]) {
  val parser = new OptionParser
  val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
    .withRequiredArg
    .describedAs("topic")
    .ofType(classOf[String])
  val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
    .withRequiredArg
    .describedAs("broker-list")
    .ofType(classOf[String])
  val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")
  val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
    "If specified without value, then it defaults to 'gzip'")
    .withOptionalArg()
    .describedAs("compression-codec")
    .ofType(classOf[String])
  val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch if they are not being sent synchronously.")
    .withRequiredArg
    .describedAs("size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(200)
  val messageSendMaxRetriesOpt = parser.accepts("message-send-max-retries", "Brokers can fail receiving the message for multiple reasons, and being unavailable transiently is just one of them. This property specifies the number of retires before the producer give up and drop this message.")
    .withRequiredArg
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(3)
  val retryBackoffMsOpt = parser.accepts("retry-backoff-ms", "Before each retry, the producer refreshes the metadata of relevant topics. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.")
    .withRequiredArg
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(100)
  val sendTimeoutOpt = parser.accepts("timeout", "If set and the producer is running in asynchronous mode, this gives the maximum amount of time" +
    " a message will queue awaiting sufficient batch size. The value is given in ms.")
    .withRequiredArg
    .describedAs("timeout_ms")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(1000)
  val timeIntervalOpt = parser.accepts("interval", "Time interval to send message to the topic in ms. Default is 5000 ms.")
    .withRequiredArg
    .describedAs("interval")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(5000)
  val queueSizeOpt = parser.accepts("queue-size", "If set and the producer is running in asynchronous mode, this gives the maximum amount of " +
    " messages will queue awaiting sufficient batch size.")
    .withRequiredArg
    .describedAs("queue_size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(10000)
  val queueEnqueueTimeoutMsOpt = parser.accepts("queue-enqueuetimeout-ms", "Timeout for event enqueue")
    .withRequiredArg
    .describedAs("queue enqueuetimeout ms")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(Int.MaxValue)
  val requestRequiredAcksOpt = parser.accepts("request-required-acks", "The required acks of the producer requests")
    .withRequiredArg
    .describedAs("request required acks")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(0)
  val requestTimeoutMsOpt = parser.accepts("request-timeout-ms", "The ack timeout of the producer requests. Value must be non-negative and non-zero")
    .withRequiredArg
    .describedAs("request timeout ms")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(1500)
  val metadataExpiryMsOpt = parser.accepts("metadata-expiry-ms",
    "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any leadership changes.")
    .withRequiredArg
    .describedAs("metadata expiration interval")
    .ofType(classOf[java.lang.Long])
    .defaultsTo(5*60*1000L)
  val maxBlockMsOpt = parser.accepts("max-block-ms",
    "The max time that the producer will block for during a send request")
    .withRequiredArg
    .describedAs("max block on send")
    .ofType(classOf[java.lang.Long])
    .defaultsTo(60*1000L)
  val maxMemoryBytesOpt = parser.accepts("max-memory-bytes",
    "The total memory used by the producer to buffer records waiting to be sent to the server.")
    .withRequiredArg
    .describedAs("total memory in bytes")
    .ofType(classOf[java.lang.Long])
    .defaultsTo(32 * 1024 * 1024L)
  val maxPartitionMemoryBytesOpt = parser.accepts("max-partition-memory-bytes",
    "The buffer size allocated for a partition. When records are received which are smaller than this size the producer " +
      "will attempt to optimistically group them together until this size is reached.")
    .withRequiredArg
    .describedAs("memory in bytes per partition")
    .ofType(classOf[java.lang.Long])
    .defaultsTo(16 * 1024L)
  val valueEncoderOpt = parser.accepts("value-serializer", "The class name of the message encoder implementation to use for serializing values.")
    .withRequiredArg
    .describedAs("encoder_class")
    .ofType(classOf[java.lang.String])
    .defaultsTo(classOf[DefaultEncoder].getName)
  val keyEncoderOpt = parser.accepts("key-serializer", "The class name of the message encoder implementation to use for serializing keys.")
    .withRequiredArg
    .describedAs("encoder_class")
    .ofType(classOf[java.lang.String])
    .defaultsTo(classOf[DefaultEncoder].getName)
  val messageReaderOpt = parser.accepts("line-reader", "The class name of the class to use for reading lines from standard in. " +
    "By default each line is read as a separate message.")
    .withRequiredArg
    .describedAs("reader_class")
    .ofType(classOf[java.lang.String])
    .defaultsTo(classOf[URLReader].getName)
  val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
    .withRequiredArg
    .describedAs("size")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(1024*100)
  val propertyOpt = parser.accepts("property", "A mechanism to pass user-defined properties in the form key=value to the message reader. " +
    "This allows custom configuration for a user-defined message reader.")
    .withRequiredArg
    .describedAs("prop")
    .ofType(classOf[String])
  val producerPropertyOpt = parser.accepts("producer-property", "A mechanism to pass user-defined properties in the form key=value to the producer. ")
    .withRequiredArg
    .describedAs("producer_prop")
    .ofType(classOf[String])
  val producerConfigOpt = parser.accepts("producer.config", s"Producer config properties file. Note that $producerPropertyOpt takes precedence over this config.")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])
  val securityProtocolOpt = parser.accepts("security-protocol", "The security protocol to use to connect to broker.")
    .withRequiredArg
    .describedAs("security-protocol")
    .ofType(classOf[String])
    .defaultsTo("PLAINTEXT")

  val inputFileOpt = parser.accepts("input-file", "Input file to generate producer records.")
    .withRequiredArg()
    .describedAs("input")
    .ofType(classOf[String])

  val options = parser.parse(args : _*)
  if(args.length == 0)
    CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
  CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, brokerListOpt)

  import scala.collection.JavaConversions._
  val inputFile = options.valueOf(inputFileOpt)
  val topic = options.valueOf(topicOpt)
  val brokerList = options.valueOf(brokerListOpt)
  ToolsUtils.validatePortOrDie(parser,brokerList)
  val sync = options.has(syncOpt)
  val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
  val compressionCodec = if (options.has(compressionCodecOpt))
    if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
      DefaultCompressionCodec.name
    else compressionCodecOptionValue
  else NoCompressionCodec.name
  val batchSize = options.valueOf(batchSizeOpt)
  val sendTimeout = options.valueOf(sendTimeoutOpt)
  val timeInterval = options.valueOf(timeIntervalOpt)
  val queueSize = options.valueOf(queueSizeOpt)
  val queueEnqueueTimeoutMs = options.valueOf(queueEnqueueTimeoutMsOpt)
  val requestRequiredAcks = options.valueOf(requestRequiredAcksOpt)
  val requestTimeoutMs = options.valueOf(requestTimeoutMsOpt)
  val messageSendMaxRetries = options.valueOf(messageSendMaxRetriesOpt)
  val retryBackoffMs = options.valueOf(retryBackoffMsOpt)
  val keyEncoderClass = options.valueOf(keyEncoderOpt)
  val valueEncoderClass = options.valueOf(valueEncoderOpt)
  val socketBuffer = options.valueOf(socketBufferSizeOpt)
  val cmdLineProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(propertyOpt))
  val extraProducerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropertyOpt))
  val maxMemoryBytes = options.valueOf(maxMemoryBytesOpt)
  val maxPartitionMemoryBytes = options.valueOf(maxPartitionMemoryBytesOpt)
  val metadataExpiryMs = options.valueOf(metadataExpiryMsOpt)
  val maxBlockMs = options.valueOf(maxBlockMsOpt)
  val securityProtocol = options.valueOf(securityProtocolOpt).toString
}