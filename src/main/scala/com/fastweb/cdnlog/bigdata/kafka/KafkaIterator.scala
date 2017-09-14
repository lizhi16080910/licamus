package com.fastweb.cdnlog.bigdata.kafka

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect._

/**
  * Created by lfq on 2016/8/18.
  */

/**
  *
  * @param kafkaParams
  * @param topic
  * @param partitionId
  * @param startOffset
  * @param endOffset
  * 该类就是获取特定startOffset与endOffset之间的数据，包括startOffset，但不包括endOffset
  *
  */
class KafkaIterator(kafkaParams: Map[String, String], topic: String, partitionId: Int, startOffset: Long, endOffset: Long) extends Iterator[MessageAndOffset] {
    val log = LoggerFactory.getLogger(KafkaIterator.getClass)
    val kc = new KafkaCluster(kafkaParams)
    var currentOffset = startOffset
    var consumer = connectLeader
    var iter = fetchBatch
    var finished = false

    // true：表示完成，false：表示未完成
    // The idea is to use the provided preferred host, except on task retry atttempts,
    // to minimize number of kafka metadata requests
    private def connectLeader: SimpleConsumer = {
        println("topic is " + this.topic)
        println("partition id  is " + this.partitionId)

        kc.connectLeader(topic, partitionId).fold(
            errs => throw new RuntimeException(
                s"Couldn't connect to leader for topic ${topic} ${partitionId}: \n, topic is " + this.topic + " partition is " + this.partitionId +
                    errs.mkString("\n")),
            consumer => consumer
        )
    }

    def config(): VerifiableProperties = {
        kc.config.props
    }

    private def handleFetchErr(resp: FetchResponse) {
//        log.info("topic is " + this.topic)
//        log.info("partition id  is " + this.partitionId)
        if (resp.hasError) {
            val err = resp.errorCode(topic, partitionId)
            if (err == ErrorMapping.LeaderNotAvailableCode ||
                err == ErrorMapping.NotLeaderForPartitionCode) {
                println(s"error: Lost leader for topic ${topic} partition ${partitionId}, " +
                    s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
                Thread.sleep(kc.config.refreshLeaderBackoffMs)
            }
            // Let normal rdd retry sort out reconnect attempts
            //            consumer = null
            //            consumer = connectLeader
            throw ErrorMapping.exceptionFor(err)
        }
    }

    def fetchBatch: Iterator[MessageAndOffset] = {
        val req = new FetchRequestBuilder()
            .addFetch(topic, partitionId, currentOffset, kc.config.fetchMessageMaxBytes) //
            .build()
        //println(kc.config.fetchMessageMaxBytes);
        var resp = consumer.fetch(req)
        handleFetchErr(resp)

        resp.messageSet(topic, partitionId)
            .iterator
            .dropWhile(_.offset < currentOffset)
    }

    def close(): Unit = {
        if (consumer != null) {
            println("consumer close")
            consumer.close()
        }
    }

    override def hasNext: Boolean = {
        var hasNext = iter.hasNext
        if (finished) {
            close()
            hasNext
            return false
        }
        if ((!hasNext)) {
            currentOffset += 1
            iter = fetchBatch
            hasNext = iter.hasNext
        }
        hasNext
    }

    override def next(): MessageAndOffset = {
        val msg = iter.next()
        currentOffset = msg.offset
        if ((currentOffset + 1) == endOffset) {
            finished = true
        }
        msg
    }
}

object KafkaIterator {

    def apply(kafkaParams: java.util.Map[String, String], topic: String, partitionId: Int, startOffset: Long, endOffset: Long): KafkaIterator = {
        new KafkaIterator(kafkaParams.asScala.toMap, topic, partitionId, startOffset, endOffset)
    }

    def main(args: Array[String]) {
        val brokerList = "115.238.138.22:9092,115.238.138.23:9092,115.238.138.24:9092," +
            "115.238.138.25:9092,115.238.138.26:9092,115.238.138.27:9092,115.238.138.28:9092,115.238.138.29:9092,115.238.138.30:9092,115.238.138.31:9092";
        val brokerList2 = "192.168.100.181:39092,192.168.100.182:39092,192.168.100.183:39092,192.168.100.184:39092"
        val brokerList3 = "192.168.100.181:19092,192.168.100.182:19092,192.168.100.183:19092,192.168.100.184:19092"

        val brokerList4 = "192.168.100.3:9092,192.168.100.4:9092,192.168.100.5:9092,192.168.100.6:9092"
        val kafkaParams = Map("metadata.broker.list" -> brokerList4)

        val kc = new KafkaCluster(kafkaParams)
        val topic = "cdnlog_topic"
        val partitionId = 46
        kc.getLatestLeaderOffsets(Set(new TopicAndPartition(topic, 0))).fold(
            err => {
                println(err.toString())
                throw new RuntimeException("error")
            },
            map => map
        ).foreach(println)
        val iter = new KafkaIterator(kafkaParams, topic, partitionId, 4047133453l,4047133455l)
        var j = 0l;
        while (iter.hasNext) {
            j += 1
            val i = iter.next()
            val keyDecoder = classTag[StringDecoder].runtimeClass.getConstructor(classOf[VerifiableProperties])
                .newInstance(kc.config.props)
                .asInstanceOf[Decoder[String]]
            val valueDecoder = classTag[StringDecoder].runtimeClass.getConstructor(classOf[VerifiableProperties])
                .newInstance(kc.config.props)
                .asInstanceOf[Decoder[String]]
            val mm = new MessageAndMetadata[String, String](topic, partitionId, i.message, i.offset, keyDecoder, valueDecoder)
            println(j + "-> " + i.offset + ":" + mm.message())
        }
        iter.close()
    }
}
