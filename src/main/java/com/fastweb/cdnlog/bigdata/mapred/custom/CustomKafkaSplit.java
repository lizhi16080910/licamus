package com.fastweb.cdnlog.bigdata.mapred.custom;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lfq on 2016/10/13.
 */
public class CustomKafkaSplit extends InputSplit implements Writable {

    //private String topic = null; // topic
    private byte[] topic = null;
    private int topicNameLength = -1; //topic 名称字符串的长度
    private int partiton = -1; //partition number

    private long startOffset = -1l;
    private long endOffset = -1l;

    public CustomKafkaSplit() {}

    public CustomKafkaSplit(String topic, int partiton,long startOffset,long endOffset) {
        this.topic = topic.getBytes();
        this.partiton = partiton;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.topicNameLength = this.topic.length;
    }

    @Override
    public String toString() {
        return "CustomKafkaSplit{" +
                "topic=" + new String(this.topic) +
                ", topicNameLength=" + topicNameLength +
                ", partiton=" + partiton +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                '}';
    }

    public CustomKafkaSplit(CustomKafkaInputFomat.EtlRequest top) {
        this.topic = top.getTopic().getBytes();
        this.partiton = top.getPartiton();
        this.startOffset = top.getStartOffset();
        this.endOffset = top.getEndOffset();
        this.topicNameLength = this.topic.length;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.endOffset - this.startOffset;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public int getPartiton() {
        return this.partiton;
    }

    public long getStartOffset() {
        return this.startOffset;
    }

    public long getEndOffset() {
        return this.endOffset;
    }

    public byte[] getTopic() {
        return this.topic;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.startOffset);
        out.writeLong(this.endOffset);
        out.writeInt(this.partiton);
        out.writeInt(this.topicNameLength);
        out.write(this.topic);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.startOffset = in.readLong();
        this.endOffset = in.readLong();
        this.partiton = in.readInt();
        this.topicNameLength = in.readInt();
        this.topic = new byte[this.topicNameLength];
        in.readFully(this.topic);
        System.out.println("CustomKafkaSplit " + new String(this.topic));
    }

}
