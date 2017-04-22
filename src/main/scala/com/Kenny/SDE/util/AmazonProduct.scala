package com.Kenny.SDE.util

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.dsl.DSL._
import org.apache.log4j.LogManager

/**
  * Created by kzhang on 4/21/17.
  */
case class AmazonProduct(url: String) extends Serializable {

  val b = new JsoupBrowser

  val shortURL = url.split("/").last

  @transient lazy private val logger = LogManager.getLogger(getClass)

  val doc = b.get(url)

  def productDescription(): Option[(String, String)] = {
    val startTime = System.currentTimeMillis()
    var retry: Int = 0

    while (retry < 4){

      val result = doc >?> element("#productDescription p").map(_.text)

      result match {
        case Some(desc: String) => {
          val endTime = System.currentTimeMillis()
          logger.debug(s"With ${retry} retry for ${shortURL}, it took ${endTime - startTime} ms to get product description: \n ${result}.")
          return Some(url, desc)
        }
        case _ => {
          if (retry < 3) {
            logger.debug(s"Will retry ${retry} time(s) after 100 ms for ${shortURL}.")
            Thread.sleep(100)
            retry += 1
          } else {
            val endTime = System.currentTimeMillis()
            logger.debug(s"After 3 retries, we cannot fetch the product description for ${shortURL}")
            retry += 1
            return None
          }

        }
      }
    }
    return None
  }

  def extractTopWord(filter: Array[String]): Option[(String, String)] = {
    val input = productDescription()
    val punctReg = "[\\p{Punct}\\s]+"
    input match {
      case None => return None
      case Some((url: String, desc: String)) => {
        val tempResult = desc.toLowerCase().split(punctReg)
        val lowerFilter = filter.map(_.toLowerCase)
        val result = tempResult.filterNot(lowerFilter.contains(_))
        return Some((url, result.mkString(" ")))
      }
    }
  }
}
