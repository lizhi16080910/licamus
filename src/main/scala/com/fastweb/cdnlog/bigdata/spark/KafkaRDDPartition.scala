package com.fastweb.cdnlog.bigdata.spark

import com.fastweb.cdnlog.bigdata.inputformat.kafka.EtlRequest
import org.apache.spark.Partition

/**
  * Created by lfq on 2016/11/18.
  */
class KafkaRDDPartition(val indexP: Int, //partition 编号
                        val request: EtlRequest) extends Partition {
    /** Number of messages this partition refers to */
    override def index: Int = indexP

    def count(): Long = request.getEndOffset - request.getStartOffset

    def topic() = request.getTopic

    def partition = request.getPartiton

    def endOffset = request.getEndOffset

    def fromOffset = request.getStartOffset
}
