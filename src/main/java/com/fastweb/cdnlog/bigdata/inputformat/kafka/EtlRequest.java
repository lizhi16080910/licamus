package com.fastweb.cdnlog.bigdata.inputformat.kafka;

/**
 * Created by lfq on 2016/11/21.
 */

import java.io.Serializable;

/**
 * 包含一个job期间内对topic，partition的起始offset和结束offset
 */
public class EtlRequest implements Serializable{
    private String topic = null;
    private int partiton = -1;
    private long startOffset = -1l;
    private long endOffset = -1l;

    public EtlRequest(String topic, int partiton, long startOffset, long endOffset) {
        this.topic = topic;
        this.partiton = partiton;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public EtlRequest(TopicPartition tp, long startOffset, long endOffset) {
        this.topic = tp.topic;
        this.partiton = tp.getPartition();
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartiton() {
        return partiton;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "EtlRequest{" +
                "topic=" + topic +
                ",partiton=" + partiton +
                ",startOffset=" + startOffset +
                ",endOffset=" + endOffset +
                '}';
    }

    public static EtlRequest convert(String str){
        int index1 = str.indexOf("{");
        int index2 = str.lastIndexOf("}");
        String str1 = str.substring(index1 + 1, index2);
        String[] str2 = str1.split(",");
        String topic = str2[0].split("=")[1];
        int partition = Integer.valueOf(str2[1].split("=")[1]);
        long startOffset = Long.valueOf(str2[2].split("=")[1]);
        long endOffset = Long.valueOf(str2[3].split("=")[1]);
        return new EtlRequest(topic, partition, startOffset, endOffset);
    }

    public static void main(String[] args) {
    }
}




