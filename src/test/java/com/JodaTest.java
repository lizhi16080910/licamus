package com;

import com.fastweb.cdnlog.bigdata.kafka.KafkaCluster;
import com.fastweb.cdnlog.bigdata.kafka.KafkaIterator;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import java.io.IOException;
import java.util.*;

/**
 * Created by lfq on 2016/10/12.
 */
public class JodaTest {

    public static void main(String[] args) throws IOException {
        test();
    }

    public static void test1(){
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list","192.168.100.181:39092,192.168.100.182:39092,192.168.100.183:39092,192.168.100.184:39092");
        KafkaCluster kc = KafkaCluster.apply(kafkaParams);
        String topic = "camus_test";
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        int partitionId = 0;
        Collection<TopicAndPartition> parts = kc.getPartitions(topics);
        for(TopicAndPartition part:parts){
            System.out.println(part.toString());

        }
        Map<TopicAndPartition,KafkaCluster.LeaderOffset> offsets = kc.getLatestLeaderOffsets(parts);
        for(Map.Entry<TopicAndPartition,KafkaCluster.LeaderOffset> entry:offsets.entrySet()){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }
    }
    public static void test(){
        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list","192.168.100.181:39092,192.168.100.182:39092,192.168.100.183:39092,192.168.100.184:39092");
        String topic = "camus_test";
        int partitionId = 0;
        KafkaCluster kc = KafkaCluster.apply(kafkaParams);
        KafkaIterator iter = KafkaIterator.apply(kafkaParams,topic,partitionId,55l,1373l);
        while (iter.hasNext()) {
            MessageAndOffset msg = iter.next();
            Decoder keyDecoder = new StringDecoder(kc.config().props());
            Decoder valueDecoder = new StringDecoder(kc.config().props());
            MessageAndMetadata mm = new MessageAndMetadata<String, String>(topic, partitionId, msg.message(), msg.offset(), keyDecoder, valueDecoder);
            System.out.println( "-> " + msg.offset() + ":" + mm.message());
        }
        iter.close();
    }


}
