package com.fastweb.cdnlog.bigdata.inputformat.kafka;

/**
 * Created by lfq on 2016/11/21.
 */
public class TopicPartition {
    String topic = null;
    int partition = -1;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicPartition that = (TopicPartition) o;

        if (partition != that.partition) return false;
        return topic.equals(that.topic);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + partition;
        return result;
    }
}
