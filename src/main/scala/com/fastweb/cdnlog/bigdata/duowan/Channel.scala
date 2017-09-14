package com.fastweb.cdnlog.bigdata.duowan

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.collection.mutable.HashMap

class Channel {
  /**
   * 存放泛域名 ext.xxx.xxx.xxx
   */
  val domains = new HashMap[String, String]
  /**
   * 存储正常域名
   */
  val leftDomains = new HashMap[String, String]

  def transform(domain: String): String = {
    var i = 0
    if ({ i = domain.indexOf('.'); i } == -1) {
      null;
    }
    val sub = domain.substring(i + 1)
    if (leftDomains.contains(domain)) {
      //返回正常域名
      domain
    } else if (domains.contains(sub)) {
      //返回泛域名
      Channel.EXT_PREFIX + sub;
    } else {
      null
    }
  }

  def getUserID(domain: String): String = {
    if (leftDomains.contains(domain)) {
      //* 正常域名
      return leftDomains.get(domain).get;
    } else if (domain.startsWith(Channel.EXT_PREFIX)) {
      //* 查找泛域名ext
      return domains.get(domain.substring(Channel.EXT_PREFIX.length())).get;
    } else {
      ""
    }
  }

  override def toString(): String = {
    super.toString() + ":" + (domains.size + leftDomains.size)
  }

}

object Channel {
  /**
   * 泛域名前缀常量
   */
  val EXT_PREFIX = "ext."

  //val CDN_CHANNEL_URL = "http://data.fastweb.com.cn/cdn_channel.conf.gz"
 // val CDN_CHANNEL_URL = "http://udap.fwlog.cachecn.net:8088/cdn_channel.conf.gz"
  val CDN_CHANNEL_URL = "http://udap.fwlog.cachecn.net:8089/udap/cdn_channel.conf.gz"
  // 时间间隔为一天,一天更新一次cdn channel 
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply(CDN_CHANNEL_URL)

  def apply(inStream: InputStream): Channel = {
    val channel = new Channel()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      val ops = tmp.split(",")
      if (tmp.startsWith(EXT_PREFIX) && ops(2).equals("2")) {
        channel.domains.put(ops(0).substring(4), ops(6))
        //channel.domains.put(ops(0), ops(6))
      } else {
        channel.leftDomains.put(ops(0), ops(6))
      }
     // channel.leftDomains.put(ops(0), ops(6))
    }
    channel
  }

  def apply(path: String): Channel = {
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

  def apply(): Channel = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("cdn_channel.conf")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): Channel = {
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
    println(Channel.current)
    println(Channel.update())
  }
}