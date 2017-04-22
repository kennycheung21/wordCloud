package com.Kenny.SDE.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import java.util.{Map => JMap}
import org.apache.log4j.LogManager

/**
  * Created by kzhang on 4/21/17.
  */
class SparkCallback extends OffsetCommitCallback () with Serializable {

  @transient lazy private val logger = LogManager.getLogger(getClass)

  def onComplete(m: JMap[TopicPartition, OffsetAndMetadata], e: Exception) {
    if (null != e) {
      logger.error("Commit failed", e)
    } else {
      logger.info("Commit sucessfully for: "+ m.toString)
    }
  }
}
