package com.fastweb.cdnlog.bigdata.spark

import java.io.FileInputStream
import java.util.{Collections, Properties}

import com.hadoop.compression.lzo.LzopCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.LzoCodec
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
/**
  * Created by lfq on 2016/11/21.
  */
object SparkJob {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf()
        loadProperty("/data1/lifq/spark-camus/camus/camus.properties", sparkConf)


        val offsetDir = "/user/cdnlog_duowan/exec/offset"
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

        sparkConf.setAppName(sparkConf.get(Constant.CAMUS_JOB_NAME) + "-" + jobTime)
        sparkConf.set(Constant.JOB_TIME,jobTime)

        val sc = new SparkContext(sparkConf)
        val normalLogCount = sc.accumulator[Long](0, "normal-log-count")
        val errorLogCount = sc.accumulator[Long](0, "error-log-count")

        val rdd = new KafkaRDD(sc, s"/user/cdnlog_duowan/exec/offset/${jobTime}/offset.info").map(r => {
            r._2
        })

        rdd.saveAsTextFile("/user/camus-test/result/" + jobTime,classOf[LzopCodec])
        // normalLogCount += rdd.count()

        println(s"in put record is ${normalLogCount.value}")

        sc.addSparkListener(new JobListener(sc))
        sc.stop()
    }

    def getJobTime(): String = {
        val offsetDir = "/user/camus-test/exec/offset"
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
