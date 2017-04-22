package com.Kenny.SDE.util

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger

import scala.io.Source

/**
  * Created by kzhang on 4/21/17.
  */
class URLReader {
  private val logger = Logger.getLogger(getClass)

  var topic: String = null

  var parseKey = false
  var keySeparator = "\t"
  var ignoreError = false

  var input: String = null

  var batchSize = 1

  def init(props: Properties) {
    topic = props.getProperty("topic")
    if(props.containsKey("parse.key"))
      parseKey = props.getProperty("parse.key").trim.toLowerCase.equals("true")
    if(props.containsKey("key.separator"))
      keySeparator = props.getProperty("key.separator")
    if(props.containsKey("ignore.error"))
      ignoreError = props.getProperty("ignore.error").trim.toLowerCase.equals("true")
    input = props.getProperty("inputFile")
    logger.info("Initialized URLReader with input = " + input)

  }

  def nextWave(): Iterator[ProducerRecord[String, Int]] = {
    /*
    Reading the url string from file and transform it as producerRecord
    */
    logger.info("Reading URL from file: " + input)
    Source.fromFile(input).getLines().map((key :String) => {
      val value: Int = 0 //dummy value for future usage
      new ProducerRecord [String, Int] (topic, key, value)
    })
  }

  def getSize(): Int = {
    logger.debug("Calculating the size of file: " + input)
    Source.fromFile(input).getLines().size
  }

  def close() {}
}
