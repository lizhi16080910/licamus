package com.fastweb.cdnlog.bigdata.inputformat.kafka;

import java.io.Serializable;

/**
 * Created by lfq on 2016/11/11.
 */
public class EtlKey implements Serializable {
    String topic = null;
    int partiton = -1;
    long offset = -1l;

    public EtlKey(String topic, int partiton, long offset) {
        this.topic = topic;
        this.partiton = partiton;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartiton() {
        return partiton;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "EtlKey{" +
                "topic='" + topic + '\'' +
                ", partiton=" + partiton +
                ", offset=" + offset +
                '}';
    }
}
