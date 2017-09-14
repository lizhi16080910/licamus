package com.fastweb.cdnlog.bigdata.spark

import com.fastweb.cdnlog.bigdata.inputformat.kafka.{EtlKey, EtlRequest}
import com.fastweb.cdnlog.bigdata.kafka.{KafkaCluster, KafkaIterator}
import com.fastweb.cdnlog.bigdata.util.FileUtil
import com.google.common.util.concurrent.RateLimiter
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by lfq on 2016/8/24.
  * 传入offset文件的信息
  */
class KafkaRDD(@transient sc: SparkContext, offsetPath: String) extends RDD[(EtlKey, String)](sc, Nil) with Logging {
    val topics = sc.getConf.get(Constant.KAFKA_WHITELIST_TOPIC).split(",").toSet

    val kafkaParams = Map("metadata.broker.list" -> sc.getConf.get(Constant.KAFKA_BROKERS))
    val kc = new KafkaCluster(kafkaParams)

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(EtlKey, String)] = {
        val part = split.asInstanceOf[KafkaRDDPartition]
        val topic = part.topic
        val partitionId = part.partition
        val endOffset = part.endOffset

        val iter = new Iterator[(EtlKey, String)]() {
            val iter2 = new KafkaIterator(kafkaParams, topic, partitionId, part.fromOffset, endOffset)

            override def hasNext: Boolean = {
                iter2.hasNext
            }

            override def next(): (EtlKey, String) = {
                val msg = iter2.next()
                val keyDecoder = new StringDecoder(iter2.kc.config.props);
                val valueDecoder = new StringDecoder(iter2.kc.config.props);
                val mm = new MessageAndMetadata[String, String](topic, partitionId, msg.message, msg.offset, keyDecoder,
                    valueDecoder)
                (new EtlKey(topic, partitionId, msg.offset), mm.message.toString())
            }
        }
        new InterruptibleIterator(context, iter)
    }


    override def count(): Long = {
        val fs = FileSystem.get(new Configuration())
        val lines = FileUtil.readHdfsFile(fs, new Path(this.offsetPath), false)
        val parts = lines.map(line => EtlRequest.convert(line)).filter(request => this.topics.contains(request.getTopic))
            .filter(request => request.getStartOffset > request.getEndOffset)
        parts.map(request => request.getEndOffset - request.getStartOffset).sum
    }

    override protected def getPartitions: Array[Partition] = {
        val fs = FileSystem.get(sc.hadoopConfiguration)
        val lines = FileUtil.readHdfsFile(fs, new Path(this.offsetPath), false)
        val parts = lines.map(line => EtlRequest.convert(line)).filter(request => this.topics.contains(request.getTopic))
            .filter(request => request.getEndOffset > request.getStartOffset)
        println(parts.length)
        val array = new Array[Partition](parts.length)
        (0 to parts.length - 1).map(i => {
            array(i) = new KafkaRDDPartition(i, parts.get(i))
        })
        array
    }

}


object KafkaRDD {
    def main(args: Array[String]) {
        val reateLimiter = RateLimiter.create(2.0)
        val source = Source.fromFile("F:\\test\\00")
        source.getLines.foreach(line => {
            reateLimiter.acquire()
            println(line)
        })
        source.close()
    }
}