package com.fastweb.cdnlog.bigdata.spark

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

/**
  * Created by lfq on 2016/11/22.
  */
class JobListener(sc:SparkContext) extends SparkListener{
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        jobEnd.jobResult match {
            case JobSucceeded =>  {
                //sc.hadoopConfiguration
                println("job finished, delete offset dir")
            }
            case _ =>
        }
        println("job finished, delete offset dir")
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    }
}
