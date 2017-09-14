package com.fastweb.cdnlog.bigdata.spark;

import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.inputformat.kafka.EtlRequest;
import com.fastweb.cdnlog.bigdata.inputformat.kafka.TopicPartition;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by lfq on 2016/11/18.
 */
public class KafkaComputeParition implements Serializable{
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String FILE_NAME = "offset.info";

    private static final Log LOG = LogFactory.getLog(KafkaComputeParition.class);

    public String offsetPath = null;
    private Map<String, String> kafkaParams = new HashMap<>();
    private List<String> topics = null;
    private Map<String, Integer> topicsMap = new HashMap<>();

    private SparkContext context = null;

    public KafkaComputeParition(SparkContext context, String offsetPath) {
        this.context = context;
        this.offsetPath = offsetPath;
    }

    public List<Partition> getParition() throws IOException, InterruptedException {
        this.kafkaParams.put(METADATA_BROKER_LIST, context.getConf().get(Constant.KAFKA_BROKERS));
        this.topics = getTopics();
        LOG.info("all topic are {" + this.topics + "}");
        List<Partition> splits = new ArrayList<>();
        int partID = 0;
        Map<TopicPartition, EtlRequest> map = getOffsetInfo(context);
        for (TopicPartition key : map.keySet()) {
            if (!this.topicsMap.containsKey(key.getTopic())) {
                continue;
            }
            EtlRequest request = map.get(key);
            long startOffset = request.getStartOffset();
            long endOffset = request.getEndOffset();
            if (startOffset >= endOffset) {
                continue;
            }
            KafkaRDDPartition part = new KafkaRDDPartition(partID, request);
            splits.add(part);
            partID++;
        }
        return splits;
    }

    /**
     * @return 获取topic的offset信息，如果存放历史记录的文件夹存在，但是没有文件，则不读取，返回一个空map
     */
    public Map<TopicPartition, EtlRequest> getOffsetInfo(SparkContext context) throws IOException {
        Configuration conf = context.hadoopConfiguration();
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(new Path(this.offsetPath))) {
            Map<TopicPartition, EtlRequest> map = new HashMap<>();
            return map;
        }
        return convertToMap(readOffsetInfo(this.offsetPath, context));
    }

    private Map<TopicPartition, EtlRequest> convertToMap(List<EtlRequest> list) {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();
        for (EtlRequest top : list) {
            String topic = top.getTopic();
            int partition = top.getPartiton();
            map.put(new TopicPartition(topic, partition), top);
        }
        return map;
    }

    private List<EtlRequest> readOffsetInfo(String path, SparkContext context) throws IOException {
        List<EtlRequest> list = new ArrayList<>();

        Path file = new Path(path);
        LOG.info("offset information file is " + file.toString());
        Configuration conf = context.hadoopConfiguration();
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(file)) {
            return list;
        }
        List<String> lines = FileUtil.readHdfsFile(fs, file,false);
        for (String str : lines) {
            if (str.isEmpty()) {
                continue;
            }
            list.add(convert(str));
        }
        //fs.delete(file, false);
        return list;
    }

    public static EtlRequest convert(String line) {
        int index1 = line.indexOf("{");
        int index2 = line.lastIndexOf("}");
        String str1 = line.substring(index1 + 1, index2);
        String[] str2 = str1.split(",");
        String topic = str2[0].split("=")[1];
        int partition = Integer.valueOf(str2[1].split("=")[1]);
        long startOffset = Long.valueOf(str2[2].split("=")[1]);
        long endOffset = Long.valueOf(str2[3].split("=")[1]);
        return new EtlRequest(topic, partition, startOffset, endOffset);
    }

    private List<String> getTopics() {
        String[] tmp = this.context.getConf().get(Constant.KAFKA_WHITELIST_TOPIC).split(",");
        for (String topic : tmp) {
            this.topicsMap.put(topic, 1);
        }
        return Arrays.asList(tmp);
    }

}
