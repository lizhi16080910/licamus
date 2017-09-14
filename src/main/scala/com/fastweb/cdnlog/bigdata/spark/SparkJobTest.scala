package com.fastweb.cdnlog.bigdata.spark

import java.io.FileInputStream
import java.util.{Collections, Properties}

import com.hadoop.compression.lzo.LzopCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by lfq on 2016/11/21.
  */
object SparkJobTest {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf()



        sparkConf.setAppName("test").setMaster("local[2]")
        sparkConf.set(Constant.JOB_TIME,"nn")

        val sc = new SparkContext(sparkConf)
//        val normalLogCount = sc.accumulator[Long](0, "normal-log-count")
//        val errorLogCount = sc.accumulator[Long](0, "error-log-count")

        val rdd = sc.textFile("F:\\test\\kans_query_stat.log").map(x => CloudxnsLog(x.trim))
        rdd.foreach(println)

        // normalLogCount += rdd.count()
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
