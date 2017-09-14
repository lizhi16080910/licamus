package com.fastweb.cdnlog.bigdata.analysis

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.collection.mutable.HashMap

/**
 * Created by zery on 15-8-17.
 */

class PlatformInfo {
  val List = new HashMap[String, (String, Int, String, Int, String, String, Int)]()

  def findIsp(hostName: String): String = {
    val platformIsp = if (List.contains(hostName)) {
      List.get(hostName).get._1
    } else {
      "-"
    }

    platformIsp
  }

  def findIspCode(hostName: String): Int= {
    val platformIspCode = if (List.contains(hostName)) {
      List.get(hostName).get._2
    } else {
      0
    }

    platformIspCode
  }

  def findPrv(hostName: String): String = {
    val platformPrv = if (List.contains(hostName)) {
      List.get(hostName).get._3
    } else {
      "-"
    }

    platformPrv
  }

  def findPrvCode(hostName: String): Int = {
    val platformPrvCode = if (List.contains(hostName)) {
      List.get(hostName).get._4
    } else {
      0
    }

    platformPrvCode
  }

  def findName(hostName: String): String= {
    val platformName = if (List.contains(hostName)) {
      List.get(hostName).get._5
    } else {
      "-"
    }

    platformName
  }

  def findIp(hostName: String) : String = {
    val platformIp = if (List.contains(hostName)) {
      List.get(hostName).get._6
    } else {
      "-"
    }

    platformIp
  }

  def findID(hostName: String): Int= {
    val platformID = if (List.contains(hostName)) {
      List.get(hostName).get._7
    } else {
      0
    }

    platformID
  }

  def listNode() = {
    var i = 0
    for ((k, v) <- List) {
      i = i + 1
      println(i, k, v._1, v._2, v._3, v._4, v._5, v._6, v._7)
    }
  }

}

object PlatformInfo {
  val platformInfoUrl = "http://udap.fwlog.cachecn.net:8088/ip_list.gz"
  val timeInterval = 1 * 60 * 60 * 1000L

  var lastUpdate = 0L
  var current = apply()

  def apply(inStream: InputStream): PlatformInfo= {
    val platform = new PlatformInfo()
    val reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    var i = 0
    while ({ tmp = reader.readLine(); tmp } != null) {
      i = i + 1
      val fields = tmp.split(",")
        val singlePlatformInfo = try {
          val hostName = fields(0)
          val hostIsp = fields(1)
          val hostIspCode = fields(2).toInt
          val hostPrv = fields(3)
          val hostPrvCode = fields(4).toInt
          val platformName = fields(5)
          val platformIp = fields(6)
          val platformID = fields(7).toInt
          (hostName, (hostIsp, hostIspCode, hostPrv, hostPrvCode, platformName, platformIp, platformID))
        } catch {
          case _: Throwable => {
            ("-", ("-", 0, "-", 0, "-", "-", 0))
          }
        }
        platform.List += singlePlatformInfo
    }

    platform
  }

  def apply(path: String): PlatformInfo= {
    val url = new URL(path)
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

  def apply(): PlatformInfo = {
    val inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("ip_list")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }

  def update(): PlatformInfo = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeInterval) {
      lastUpdate = currentTime
      apply(platformInfoUrl)
    } else {
      current
    }
  }

  def main(args: Array[String]) {
    lastUpdate = System.currentTimeMillis()
    val platform = PlatformInfo.update()
    platform.listNode()
  }
}