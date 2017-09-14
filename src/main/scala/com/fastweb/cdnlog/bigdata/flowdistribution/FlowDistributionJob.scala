package com.fastweb.cdnlog.bigdata.flowdistribution

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Collections, Properties}

import com.fastweb.cdnlog.bigdata.analysis.{Channel, LogInfo}
import com.fastweb.cdnlog.bigdata.spark.{Constant, KafkaRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.sparkRDDFunctions

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by liuming on 2017/02/10.
  */
object FlowDistributionJob extends Logging {
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
            LogInfo(x._2.trim(),Channel.update())
        })

      //  sc.addSparkListener(new XNSJobListener(sc,offsetInfoDir))
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
