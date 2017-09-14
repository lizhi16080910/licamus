package com.fastweb.cdnlog.bigdata.analysis

import java.io.FileInputStream
import java.util
import java.util.{Collections, Properties}

import com.fastweb.cdnlog.bigdata.spark.{Constant, KafkaRDD}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by lfq on 2016/11/21.
  */
object SparkJob {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf()

        loadProperty("/hadoop/cdnlog/lifq/camus-spark/camus.properties", sparkConf)

        val offsetDir = "/user/cloudxns/exec/offset/"
        val conf = new Configuration()
//        conf.set("fs.defaultFS", "hdfs://nameservice1");
        val fs = FileSystem.get(conf)
        val offsetDirP: Path = new Path(offsetDir)

        val fsts = fs.listStatus(new Path("/user/cloudxns/exec/offset/"))

        if (fsts.length == 0) {
            sys.exit(1)
        }
        val dates = new ArrayBuffer[String]
        fsts.foreach(file => dates += file.getPath.getName)
        Collections.sort(dates)
        val jobTime = dates.get(0)

        sparkConf.setAppName(sparkConf.get(Constant.CAMUS_JOB_NAME) + "-" + jobTime)
        sparkConf.set(Constant.JOB_TIME, jobTime)
        sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.eventLog.enabled","true")
                .set("spark.eventLog.dir","hdfs://nameservice1/user/spark/applicationHistory/")


        val sc = new SparkContext(sparkConf)

        val rdd = new KafkaRDD(sc, s"/user/cloudxns/exec/offset/${jobTime}/offset.info").map(x => x._2)
//        rdd.mapPartitionsWithIndex( (i,iter) => {
//              iter
//        })

        rdd.saveAsTextFile("/user/camus-test/result/",classOf[GzipCodec])

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
