package com.Kenny.SDE.util

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.log4j.Logger

/**
  * Created by kzhang on 4/21/17.
  */
class ProducerCallback(var topic: String, var key: String, val value: Int) extends Callback {

  private val logger = Logger.getLogger(getClass)

  def onCompletion(metadata: RecordMetadata, e: Exception) {
    if (e != null) {
      logger.error("Error when sending message to topic {} with key: {}, value: {} with error:", topic, key, value.toString, e)
    }
  }
}
