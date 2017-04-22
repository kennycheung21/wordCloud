package com.Kenny.SDE.util

/**
  * Created by kzhang on 4/21/17.
  */
case class WordCloudParams(batch: Int = 60, switch: Int = 2, propertiesFile: String = "conf/wordCloud-defaults.conf", checkpointDir: String = "/tmp/checkpoint") {
  override def toString : String = {
    val paramValue = "Batch: " + batch.toString + "\n" + "Switch: " + switch.toString + "\n" + "My Properties: "+ propertiesFile + "\n"  + "Checkpoint Dir: " + checkpointDir + "\n"
    paramValue
  }
}
