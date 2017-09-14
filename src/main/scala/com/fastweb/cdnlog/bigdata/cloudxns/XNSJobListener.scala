package com.fastweb.cdnlog.bigdata.cloudxns

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

/**
  * Created by lfq on 2016/11/22.
  */
class XNSJobListener(sc:SparkContext,path:String) extends SparkListener{
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        val conf = sc.hadoopConfiguration
        val fs = FileSystem.get(conf)
        fs.delete(new Path(path),true)

    }
}
