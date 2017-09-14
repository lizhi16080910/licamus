package com.fastweb.cdnlog.bigdata.cloudxns

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * Created by liuming on 2017/2/10.
  */
case class CloudxnsLog(logtype: String, host: String, timestamp: Long, userid: Int, zone: String, lvn: Int, pan:
Int, view:
                       Int, count: Long, machine: String, isp: Int, region: Int)

object CloudxnsLog {

    var useridMap = {

        //获取userid 文件
        val map = new mutable.HashMap[String, Int]()
        val useridFile = Source.fromURL("http://udap.fwlog.cachecn.net:8088/xns.userid")
//        val useridFile = Source.fromFile("/data1/cloudxns_config/xns.userid")
        useridFile.getLines().foreach(line => {
            val lineArray = line.split(" ")
            map.put(lineArray(2), lineArray(0).toInt)
        })
        useridFile.close()
        map.toMap
    }

    val viewMap = {
        //获取isp region 文件
        val map = new mutable.HashMap[Int, (Int, Int)]()
        val useridFile = Source.fromURL("http://udap.fwlog.cachecn.net:8088/view_isp_region.conf")
//        val useridFile = Source.fromFile("/data1/cloudxns_config/view_isp_region.conf")
        useridFile.getLines().foreach(line => {
            val lineArray = line.split(",")
            map.put(lineArray(4).toInt, (lineArray(5).toInt, lineArray(6).toInt))
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

    def getHostDomain(queryDomain: String, lvn: Int): (String, String, Int) = {
        //val domain = "shop3.x.com.cn."
        val temp = queryDomain.split("\\.")
        var dom = ""
        if (temp.length >= lvn) {
            for (i <- (temp.length - lvn + 1) to (temp.length - 1)) {
                dom += temp(i) + "."
            }
            var host = ""
            for (i <- 0 to (temp.length - lvn)) {
                host += temp(i) + "."
            }
            val userid = useridMap.get(dom).get
            (host, dom, userid)
        } else if (temp.length == lvn - 1) {
            val userid = useridMap.get(queryDomain).get
            ("@", queryDomain, userid)
        } else {
            null
        }
    }


    def parseLine(line: String): List[CloudxnsLog] = {

        val logArray = line.split(" ")

        val machine = logArray(0)

        val logtype = logArray(1)

        val timesL = logArray(2).split(":")(1).toLong

        //时间规整1点的数据  属于1:59~2:00
        val timestamp = timesL- timesL % 3600

        var querydomain = logArray(3).split(":")(1)

        val lvn = logArray(4).split(":")(1).toInt

        val temp = getHostDomain(querydomain, lvn)
        val host = temp._1
        val userid = temp._3
        val zone = temp._2
        val pan = logArray(5).split(":")(1).toInt



        val statistic = logArray(6).split(":")(1).split("\\|")
        val list = new ArrayBuffer[CloudxnsLog]()
        for (viewcount <- statistic) {
            val view = viewcount.split("#")(0).toInt
            val ispRegion = viewMap.get(view).get
            val isp = ispRegion._1
            val region = ispRegion._2
            val count = viewcount.split("#")(1).toLong
            list += CloudxnsLog(logtype, host, timestamp, userid, zone, lvn, pan, view, count, machine, isp, region)
        }
        list.toList
    }

    def main(args: Array[String]): Unit = {
//        val source = Source.fromFile("F:\\kans_query_stat.log")
//        source.getLines().foreach(line => {
//            CloudxnsLog(line.trim) match {
//                case Left(result) => println(result)
//                case Right(line) => println(line)
//            }
//        })
//        source.close()

        val line = "oth-a_na-054-153-030-023 kanslog ts:1488520957 query:yhqhome.com. lvn:3 pan:0 statistic:160#1|"
        parseLine(line)
    }
}


