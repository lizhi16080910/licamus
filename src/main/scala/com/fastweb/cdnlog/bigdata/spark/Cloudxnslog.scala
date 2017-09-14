package com.fastweb.cdnlog.bigdata.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
  * Created by liuming on 2017/2/10.
  */
case class CloudxnsLog(logtype: String, timestamp: Long, userid: Int, querydomain: String, lvn: Int, pan: Int, view: String, count: Long)

object CloudxnsLog {

  var useridMap = {

    //获取userid 文件
    val map = new mutable.HashMap[String, Int]()
    val useridFile = Source.fromURL("http://192.168.100.42/tpr-wow/xns.userid")

    useridFile.getLines().foreach(line => {
      val lineArray = line.split(" ")
      map.put(lineArray(2), lineArray(0).toInt)
    })
    useridFile.close()
    map.toMap
  }

  def apply(line: String): Either[List[CloudxnsLog], String] = {

    //解析日志
    Try(parseLine(line)) match {
      case Success(result) => Left(result)
      case Failure(e) => Right(line)
    }
  }


  def parseLine(line: String): List[CloudxnsLog] = {

    val LogArray = line.split(" ")

    val logtype = LogArray(0)

    val timestamp = LogArray(1).split(":")(1).toLong

    var querydomain = LogArray(2).split(":")(1)

    //判断others域名
    if (querydomain.startsWith("#others#")) {
      querydomain = querydomain.substring(9)
    }

    val userid = useridMap.get(querydomain).get

    val lvn = LogArray(3).split(":")(1).toInt

    val pan = LogArray(4).split(":")(1).toInt

    val statistic = LogArray(5).split(":")(1).split("\\|")
    val list = new ArrayBuffer[CloudxnsLog]()
    for (viewcount <- statistic) {
      val view = viewcount.split("#")(0)
      val count = viewcount.split("#")(1).toLong
      list += CloudxnsLog(logtype, timestamp, userid, querydomain, lvn, pan, view, count)
    }
    list.toList
  }


  def main(args: Array[String]): Unit = {
    val line = "kpan:0 statistic:2#1|90#1|134#1|"

    println(CloudxnsLog(line))
  }
}


