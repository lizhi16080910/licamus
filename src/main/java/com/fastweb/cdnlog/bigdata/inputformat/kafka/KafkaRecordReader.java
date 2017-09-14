package com.fastweb.cdnlog.bigdata.inputformat.kafka;

import com.fastweb.cdnlog.bigdata.kafka.KafkaCluster;
import com.fastweb.cdnlog.bigdata.kafka.KafkaIterator;
import com.fastweb.cdnlog.bigdata.mapred.Constant;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lfq on 2016/10/13.
 */
public class KafkaRecordReader extends RecordReader<EtlKey, Text> {
    private static final Log LOG = LogFactory.getLog(KafkaRecordReader.class);
    private Map<String, String> kafkaParams = null;
    private EtlKey key = null;
    private Text value = null;
    private KafkaIterator iter = null;
    private KafkaCluster kc =null;
    private String topic = null;
    private int partitionId = -1;
    private long length = 0l;
    private long currentOffset = 0l;
    private long startOffset = -1l;
    private long endOffset = -1l;

    public KafkaRecordReader() {
        this.kafkaParams = new HashMap<>();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        kafkaParams.put(KafkaInputFomat.METADATA_BROKER_LIST,context.getConfiguration().get(Constant.KAFKA_BROKERS));

        KafkaSplit csplit = (KafkaSplit)split;
        this.topic = new String(csplit.getTopic());
        this.partitionId = csplit.getPartiton();
        this.startOffset = csplit.getStartOffset();
        this.endOffset = csplit.getEndOffset();
        this.length = this.endOffset - this.startOffset;
        System.out.println("topic is " + this.topic + ";");
        System.out.println("partitionId is " + this.partitionId + ";");
        System.out.println("startOffset is " + this.startOffset + ";");
        System.out.println("endOffset is " + this.endOffset + ";");
        this.iter = KafkaIterator.apply(this.kafkaParams,this.topic,this.partitionId,this.startOffset,this.endOffset);
        this.kc = KafkaCluster.apply(this.kafkaParams);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(iter.hasNext()){
            MessageAndOffset msg = iter.next();
            this.currentOffset = msg.offset();
            this.key = new EtlKey(this.topic,this.partitionId,msg.offset());
            Decoder keyDecoder = new StringDecoder(kc.config().props());
            Decoder valueDecoder = new StringDecoder(kc.config().props());
            MessageAndMetadata mm = new MessageAndMetadata<String, String>(topic, partitionId, msg.message(), msg.offset(), keyDecoder, valueDecoder);
            this.value = new Text(mm.message().toString());
            return true;
        }
        return false;
    }

    @Override
    public EtlKey getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (this.currentOffset - this.startOffset) / (this.length);
    }

    @Override
    public void close() throws IOException {
        if(iter!=null){
            iter.close();
        }
    }
}
