package com.fastweb.cdnlog.bigdata.cloudxns

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Collections, Properties}

import com.fastweb.cdnlog.bigdata.spark.{Constant, KafkaRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liuming on 2017/02/10.
  */
object CloudxnsJob extends Logging {

    private[this] val log = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]) {
        val sparkConf = new SparkConf()
        loadProperty("/hadoop/cdnlog/liuming/cloudxns/camus.properties", sparkConf)

        val offsetDir = "/user/cloudxns/exec/offset"
        val fs = FileSystem.get(new Configuration())
        val offsetDirP: Path = new Path(offsetDir)

        val fsts = fs.listStatus(offsetDirP)
        if (fsts.length == 0) {
            sys.exit(1)
        }
        val dates = new ArrayBuffer[String]
        fsts.foreach(file => dates += file.getPath.getName)
        Collections.sort(dates)
        val jobTime = dates.get(0)

        //jobtime时间格式转换为long
        val sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm")
        val sdftime = sdf.parse(jobTime)
        val sdfjobtime =sdftime.getTime/1000

        val offsetInfoDir =  s"/user/cloudxns/exec/offset/${jobTime}"
        val offsetInfoPath = s"/user/cloudxns/exec/offset/${jobTime}/offset.info"


        sparkConf.setAppName(sparkConf.get(Constant.CAMUS_JOB_NAME) + "-" + jobTime)

        sparkConf.set(Constant.JOB_TIME, jobTime)
            .set("spark.eventLog.enabled", "true")
            .set("spark.eventLog.dir", "hdfs://nameservice1/user/spark/applicationHistory/")

        val sc = new SparkContext(sparkConf)

        val rdd = new KafkaRDD(sc, offsetInfoPath).map(x => {
            CloudxnsLog(x._2.trim())
        })
        rdd.cache()


        log.info("consume count "+jobTime+" "+rdd.count())

        val normalRdd = rdd.filter(x => x.isLeft).flatMap(x => x.left.get)
        val errorRdd = rdd.filter(x => x.isRight).map(x => x.right.get).repartition(1)

        val result = normalRdd.map(xnslog => {
            val key = (xnslog.timestamp, xnslog.host, xnslog.userid, xnslog.view, xnslog.zone,xnslog.isp,xnslog.region)
            val count = xnslog.count
            (key, count)
        }).reduceByKey(_ + _).map(x => {
            Map("jobtime" -> sdfjobtime,
                "timestamp" -> x._1._1,
                "host" -> x._1._2,
                "userid" -> x._1._3,
                "viewid" -> x._1._4,
                "zone" -> x._1._5,
                "count" -> x._2,
                "isp" -> x._1._6,
                "region" -> x._1._7
            )
        })


        result.saveToEs("cdnlog.cloudxns" + "/" + jobTime.replace("-","").substring(0,8))
        val tt = jobTime.replace("-","/")
        try {
            errorRdd.saveAsTextFile(s"/user/cloudxns/error/${tt}")
        } catch {
            case e: Throwable => this.logError(s"save hdfs failed:/user/cloudxns/error/${tt}", e)
        }

        sc.addSparkListener(new XNSJobListener(sc,offsetInfoDir))
        sc.stop()
    }

    def getJobTime(): String = {
        val offsetDir = "/user/cloudxns/exec/offset"
        val fs = FileSystem.get(new Configuration())
        val offsetDirP: Path = new Path(offsetDir)

        val fsts = fs.listStatus(offsetDirP)
        if (fsts.length == 0) {
            return ""
        }
        val dates = new ArrayBuffer[String]
        fsts.foreach(file => dates += file.getPath.getName)
        Collections.sort(dates)
        dates.get(0)
    }

    def loadProperty(path: String, sparkConf: SparkConf): Unit = {
        val props = new Properties()
        val inputStream = new FileInputStream(path)
        props.load(inputStream)
        inputStream.close()
        props.keySet().foreach(key => {
            sparkConf.set(key.asInstanceOf[String], props.getProperty(key.asInstanceOf[String]))
        })
    }

}
