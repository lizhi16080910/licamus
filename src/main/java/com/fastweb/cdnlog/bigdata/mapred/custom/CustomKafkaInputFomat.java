package com.fastweb.cdnlog.bigdata.mapred.custom;

import com.fastweb.cdnlog.bigdata.kafka.KafkaCluster;
import com.fastweb.cdnlog.bigdata.mapred.Constant;
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
public class CustomKafkaInputFomat extends InputFormat<CustomKafkaInputFomat.EtlKey, Text> {
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String FILE_NAME = "offset.info";
    private static final Log LOG = LogFactory.getLog(CustomKafkaInputFomat.class);

    private Map<String, String> kafkaParams = new HashMap<>();
    private List<String> topics = new ArrayList<>();

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        this.kafkaParams.put(METADATA_BROKER_LIST, context.getConfiguration().get(Constant.KAFKA_BROKERS));
        this.topics.addAll(getTopics(context));
        LOG.info("all topic are {" + this.topics + "}");
        List<InputSplit> splits = new ArrayList<>();
        //Map<TopicPartition, EtlRequest> map = getCurrentOffsetRequest(context);
        Map<TopicPartition, EtlRequest> map = getPrevious(context);
        for (TopicPartition key : map.keySet()) {
            EtlRequest request = map.get(key);
            long startOffset = request.getStartOffset();
            long endOffset = request.getEndOffset();
            if (startOffset >= endOffset) {
                continue;
            }
            CustomKafkaSplit split = new CustomKafkaSplit(map.get(key));
            //LOG.info(split);
            splits.add(split);
        }
        //writeCurrentRequest(context, map); //将请求的offset记录保存到文件中
        return splits;
    }

    private List<String> getTopics(JobContext context) {
        Configuration conf = context.getConfiguration();
        String[] tmp = conf.get(Constant.KAFKA_WHITELIST_TOPIC).split(",");
        return Arrays.asList(tmp);
    }

    /**
     * @param
     * @return 根据brokerList，获取该topic的earlistOffset 和 latestOffset
     */
    public List<EtlRequest> getTopicInfo() {
        return getTopicInfo(this.kafkaParams);
    }

    private Map<TopicPartition, EtlRequest> convertToMap(List<EtlRequest> list) {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();
        for (EtlRequest top : list) {
            String topic = top.topic;
            int partition = top.getPartiton();
            map.put(new TopicPartition(topic, partition), top);
        }
        return map;
    }

    /**
     * @param context
     * @return
     * @throws IOException 根据上一个任务运行所产生的topic，partition，offset信息，生成本次job所需要的topic，partition，offset请求信息，
     *                     本次job的起始offset是上一次job的结束offset
     */
    private Map<TopicPartition, EtlRequest> getCurrentOffsetRequest(JobContext context) throws IOException {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();
        Map<TopicPartition, EtlRequest> previous = getPrevious(context);
        LOG.info("previous offset is " + previous);
        KafkaCluster kc = KafkaCluster.apply(this.kafkaParams);
        Collection<TopicAndPartition> parts = kc.getPartitions(this.topics);
        LOG.info("TopicAndPartition " + parts);
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> latestLeaderOffsets = kc.getLatestLeaderOffsets(parts);
        for (Map.Entry<TopicAndPartition, KafkaCluster.LeaderOffset> entry : latestLeaderOffsets.entrySet()) {
            TopicAndPartition pp = entry.getKey();
            String topic = pp.topic();
            int partition = pp.partition();
            long latestOffset = entry.getValue().offset();
            TopicPartition tp = new TopicPartition(topic, partition);
            EtlRequest topPrevious = previous.get(tp);
            LOG.info("previous :" + topPrevious);
            long endOffsetPrevious = -1l;
            if (topPrevious == null) {
                endOffsetPrevious = latestOffset;
                LOG.info("previous is null :" + topPrevious);
            } else {
                endOffsetPrevious = topPrevious.getEndOffset();
            }
            EtlRequest topCurrent = new EtlRequest(tp, endOffsetPrevious, latestOffset);
            LOG.info("current :" + topCurrent);
            map.put(tp, topCurrent);
        }
        LOG.info(map);
        return map;

    }

    /**
     * @param context
     * @return
     * @throws IOException 获取上一次任务的执行信息，如果存放历史记录的文件夹存在，但是没有文件，则不读取，返回一个空map
     */
    public Map<TopicPartition, EtlRequest> getPrevious(JobContext context) throws IOException {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();

        Configuration conf = context.getConfiguration();
        String historyDir = conf.get(Constant.ETL_EXECUTION_HISTORY_PATH);
        FileSystem fs = FileSystem.get(conf);
        Path historyDirP = new Path(historyDir);
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
        return convertToMap(readPrevious(dates.get(0), context));
    }

    private List<EtlRequest> readPrevious(String path, JobContext context) throws IOException {
        List<EtlRequest> list = new ArrayList<>();

        Path file = new Path(path + Path.SEPARATOR + FILE_NAME);
        LOG.info("previous offset file is " + file.toString());
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

    private void writeCurrentRequest(JobContext context, Map<TopicPartition, EtlRequest> map) throws IOException {
        List<String> strs = new ArrayList<>();
        for (TopicPartition key : map.keySet()) {
            strs.add(map.get(key).toString());
        }
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String destPath = conf.get("etl.execution.history.path") + Path.SEPARATOR + conf.get("job_time") + Path.SEPARATOR + FILE_NAME;
        FileUtil.writeListToHdfsFile(fs, new Path(destPath), strs);
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
        return new CustomKafkaRecordReader();
    }


    /**
     * 包含一个job期间内对topic，partition的起始offset和结束offset
     */
    public static class EtlRequest {
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
    }

    /**
     * InputFormat的key，以topic，partition，offset组成
     */
    public static class EtlKey {
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
    }

    public static class TopicPartition {
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

    public static void main(String[] args) {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(METADATA_BROKER_LIST, "192.168.100.181:39092,192.168.100.182:39092,192.168.100.183:39092,192.168.100.184:39092");
        List<EtlRequest> tops = new CustomKafkaInputFomat().getTopicInfo(kafkaParams);
        for (EtlRequest top : tops) {
            System.out.println(top);
        }
        System.out.println(convert("TopicOffsetPartition{topic=camus_test,partiton=0,startOffset=1373,endOffset=1373}"));
        System.out.println(convert("TopicOffsetPartition{topic=camus_test,partiton=1,startOffset=69,endOffset=17573}"));
    }
}
