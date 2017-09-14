package com.fastweb.cdnlog.bigdata.analysis

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.collection.mutable.HashMap

class IPArea {
  private val areas = new HashMap[String, (String, String)]

  def getPrvAndCountry(city: String): (String, String) = {
    var area = areas.get(city)
    if (area.isEmpty) {
      ("0", "0")
    } else {
      area.get
    }
  }
  def hasCityCode(city: String): Boolean = {
    areas.contains(city)
  }
  override def toString(): String = {
    super.toString() + ":" + (areas.size)
  }

}

object IPArea {
  val CDN_CHANNEL_URL = "http://192.168.100.204:8088/ip_area.gz"
  //val CDN_CHANNEL_URL = "http://data.fastweb.com.cn/cdn_channel.conf.gz"
  // 时间间隔为一天,一天更新一次cdn channel 
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply()

  def apply(inStream: InputStream): IPArea = {
    val ipArea = new IPArea()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      val ops = tmp.split(",")
      if (ops.length == 3) {
        ipArea.areas.put(ops(0), (ops(1), ops(2)))
      }
    }
    ipArea
  }

  def apply(path: String): IPArea = {
    var url = new URL(path)
    try {
      val gz = new GZIPInputStream(url.openStream())
      try {
        current = this(gz)
        return current
      } catch {
        case e: Throwable => {
          println(e)
          return current
        }
      } finally {
        gz.close()
      }
    } catch {
      case e: Throwable => {
        println(e)
        return current
      }
    }
  }

  def apply(): IPArea = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("ip_area")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): IPArea = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(CDN_CHANNEL_URL)
    } else {
      current
    }
  }
  def main(args: Array[String]) {
    lastUpdate = 0
    println(IPArea.current)
    println(IPArea.update())
    println(IPArea.current)
    println(IPArea.update())
    println(IPArea.update().getPrvAndCountry("865370"))
  }
}