package com.fastweb.cdnlog.bigdata.analysis

/**
 * Created by zhangyi on 2015/12/29.
 */

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util
import java.util.zip.GZIPInputStream

class ParentIP {
   val  IPlist = new util.ArrayList[String]


}

object ParentIP {
  val PARENT_IP_URL = "http://data.fastweb.com.cn/parent_ip.gz"
  //val CDN_CHANNEL_URL = "http://data.fastweb.com.cn/cdn_channel.conf.gz"
  // 时间间隔为一小时更新一次
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply()

  def apply(inStream: InputStream): ParentIP = {
    val ParentIP = new ParentIP()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      val ops = tmp.split(",")
      if (ops.length == 1) {
        ParentIP.IPlist.add(ops(0))
      }
    }
        ParentIP
      }

      def apply(path: String): ParentIP = {
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

  def apply(): ParentIP = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("parent_ip")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): ParentIP = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(PARENT_IP_URL)
    } else {
      current
    }
  }
  def main(args: Array[String]) {
    lastUpdate = 0
    println(ParentIP.current.IPlist)
   if(ParentIP.current.IPlist.contains("101.69.115.17").toString == "true"){
     println("aa")
   }
  }
}
