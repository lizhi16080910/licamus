package com.fastweb.cdnlog.bigdata.inputformat.kafka;

import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.kafka.KafkaCluster;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import kafka.common.TopicAndPartition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by lfq on 2016/10/13.
 */
public class KafkaInputFomat extends InputFormat<EtlKey, Text> {
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String FILE_NAME = "offset.info";

    private static final Log LOG = LogFactory.getLog(KafkaInputFomat.class);

    public static String offsetPath = null; //通过set方法初始化kafkaParams
    private Map<String, String> kafkaParams = new HashMap<>();
    private List<String> topics = null;
    private Map<String, Integer> topicsMap = new HashMap<>();

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        this.kafkaParams.put(METADATA_BROKER_LIST, context.getConfiguration().get(Constant.KAFKA_BROKERS));
        this.topics = getTopics(context);
        LOG.info("all topic are {" + this.topics + "}");
        List<InputSplit> splits = new ArrayList<>();

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
            KafkaSplit split = new KafkaSplit(map.get(key));
            splits.add(split);
        }
        return splits;
    }

    private List<String> getTopics(JobContext context) {
        Configuration conf = context.getConfiguration();
        String[] tmp = conf.get(Constant.KAFKA_WHITELIST_TOPIC).split(",");
        for (String topic : tmp) {
            this.topicsMap.put(topic, 1);
        }
        return Arrays.asList(tmp);
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


    /**
     * @param context
     * @return
     * @throws IOException 获取topic的offset信息，如果存放历史记录的文件夹存在，但是没有文件，则不读取，返回一个空map
     */
    public Map<TopicPartition, EtlRequest> getOffsetInfo2(JobContext context) throws IOException {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();

        Configuration conf = context.getConfiguration();
        String offsetDir = conf.get(Constant.ETL_EXECUTION_OFFSET_PATH);
        FileSystem fs = FileSystem.get(conf);
        Path historyDirP = new Path(offsetDir);
        if (!fs.exists(historyDirP)) {
            return map;
        }
        FileStatus[] fsts = fs.listStatus(historyDirP);
        if (fsts.length == 0) {
            return map;
        }
        List<String> dates = new ArrayList<>();
        for (FileStatus fst : fsts) {
            dates.add(fst.getPath().toString());
        }
        Collections.sort(dates);
        return convertToMap(readOffsetInfo(dates.get(0), context));
    }

    /**
     *
     * @return
     *  获取topic的offset信息，如果存放历史记录的文件夹存在，但是没有文件，则不读取，返回一个空map
     */
    public Map<TopicPartition, EtlRequest> getOffsetInfo(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        if(offsetPath == null){
            throw new RuntimeException("offset path is not set");
        }
        if(!fs.exists(new Path(offsetPath))){
            Map<TopicPartition, EtlRequest> map = new HashMap<>();
            return map;
        }
        return convertToMap(readOffsetInfo(offsetPath, context));
    }

    private List<EtlRequest> readOffsetInfo(String path, JobContext context) throws IOException {
        List<EtlRequest> list = new ArrayList<>();

        Path file = new Path(path);
        LOG.info("offset information file is " + file.toString());
        Configuration conf = context.getConfiguration();
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

    public List<EtlRequest> getTopicInfo(Map<String, String> kafkaParams) {
        KafkaCluster kc = KafkaCluster.apply(kafkaParams);

        Collection<TopicAndPartition> parts = kc.getPartitions(topics);
        LOG.info("TopicAndPartition " + parts);
        List<EtlRequest> splits = new ArrayList<>();
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> latestLeaderOffsets = kc.getLatestLeaderOffsets(parts);
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> earliestLeaderOffsets = kc.getEarliestLeaderOffsets(parts);
        for (Map.Entry<TopicAndPartition, KafkaCluster.LeaderOffset> entry : latestLeaderOffsets.entrySet()) {
            TopicAndPartition pp = entry.getKey();
            String topic = pp.topic();
            int partition = pp.partition();
            long latestOffset = entry.getValue().offset();
            long earliestOffset = earliestLeaderOffsets.get(pp).offset();

            splits.add(new EtlRequest(topic, partition, earliestOffset, latestOffset));
        }
        return splits;
    }

    @Override
    public RecordReader<EtlKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        LOG.info("create CustomKafkaRecordReader");
        System.out.println("create CustomKafkaRecordReader");
        return new KafkaRecordReader();
    }

    public static void setOffsetInputPath(String path){
        offsetPath = path;
    }

    public static String getOffsetInputPath(String path){
        return offsetPath;
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

    public static void main(String[] args) {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(METADATA_BROKER_LIST, "192.168.100.181:39092,192.168.100.182:39092,192.168.100.183:39092,192.168.100.184:39092");
        List<EtlRequest> tops = new KafkaInputFomat().getTopicInfo(kafkaParams);
        for (EtlRequest top : tops) {
            System.out.println(top);
        }
        System.out.println(convert("TopicOffsetPartition{topic=camus_test,partiton=0,startOffset=1373,endOffset=1373}"));
        System.out.println(convert("TopicOffsetPartition{topic=camus_test,partiton=1,startOffset=69,endOffset=17573}"));
    }
}
