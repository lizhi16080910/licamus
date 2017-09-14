package com.fastweb.cdnlog.bigdata.duowan;

import com.fastweb.cdnlog.bigdata.kafka.KafkaCluster;
import com.fastweb.cdnlog.bigdata.mapred.custom.CustomKafkaInputFomat.EtlRequest;
import com.fastweb.cdnlog.bigdata.mapred.custom.CustomKafkaInputFomat.TopicPartition;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import kafka.common.TopicAndPartition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by lfq on 2016/10/20.
 * 这个类主要是结合shell脚本定时启动，记录kafka topic在该时间的latestoffset
 */
public class OffsetRecorder {
    private static final Log LOG = LogFactory.getLog(OffsetRecorder.class);
    public static final String METADATA_BROKER_LIST = "metadata.broker.list";
    public static final String FILE_NAME = "offset.info";
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
    private String offsetRecorderPath = null;
    private String offsetRecorderPathLocal = null;
    private List<String> topics = null;
    private String brokerList = null;
    private Map<String, String> kafkaParams = null;
    private String time = null;
    private String lastTime = null;

    public OffsetRecorder(long time, long lastTime, String offsetRecorderPath, List<String> topics, String brokerList) {
        this.time = sdf.format(new Date(time * 1000));
        this.lastTime = sdf.format(new Date(lastTime * 1000));
        this.offsetRecorderPath = offsetRecorderPath;
        this.topics = topics;
        this.brokerList = brokerList;
        this.kafkaParams = new HashMap<>();
        this.kafkaParams.put(METADATA_BROKER_LIST, this.brokerList);
    }

    public OffsetRecorder(long time, long lastTime, String offsetRecorderPath, List<String> topics, String brokerList, String offsetRecorderPathLocal) {
        this.time = sdf.format(new Date(time * 1000));
        this.lastTime = sdf.format(new Date(lastTime * 1000));
        this.offsetRecorderPath = FileUtil.pathPreProcess(offsetRecorderPath);
        this.topics = topics;
        this.brokerList = brokerList;
        this.kafkaParams = new HashMap<>();
        this.kafkaParams.put(METADATA_BROKER_LIST, this.brokerList);
        this.offsetRecorderPathLocal = offsetRecorderPathLocal;
    }

    public OffsetRecorder(long time, long lastTime, String offsetRecorderPath, List<String> topics, Map<String, String> kafkaParams) {
        this.time = sdf.format(new Date(time * 1000));
        this.lastTime = sdf.format(new Date(lastTime * 1000));
        this.offsetRecorderPath = FileUtil.pathPreProcess(offsetRecorderPath);
        this.kafkaParams = kafkaParams;
        this.topics = topics;
    }

    public List<EtlRequest> getPrevious() throws IOException {
        String path = this.offsetRecorderPath + Path.SEPARATOR + this.lastTime + Path.SEPARATOR + FILE_NAME;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(new Path(path))) {
            return null;
        }
        return readPrevious(path, conf);
    }

    public List<EtlRequest> getPreviousFromLocal() {
        String path = this.offsetRecorderPathLocal + File.separator + this.lastTime + File.separator + FILE_NAME;
        LOG.info("previous request file path is :" + path);
        List<EtlRequest> list = new ArrayList<>();
        File file = new File(path);
        if (!file.exists()) {
            LOG.info("previous request file path not exist :" + path);
            LOG.info("return a empty list");
            return list;
        }
        List<String> lines = FileUtil.readFile(file);
        for (String str : lines) {
            if (str.isEmpty()) {
                continue;
            }
            list.add(convert(str));
        }
        return list;
    }

    private List<EtlRequest> readPrevious(String path, Configuration conf) throws IOException {
        List<EtlRequest> list = new ArrayList<>();
        Path file = new Path(path);
        LOG.info("previous offset file is " + file.toString());
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

    private Map<TopicPartition, EtlRequest> convertToMap(List<EtlRequest> list) {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();
        for (EtlRequest top : list) {
            String topic = top.getTopic();
            int partition = top.getPartiton();
            map.put(new TopicPartition(topic, partition), top);
        }
        return map;
    }

    private Map<TopicPartition, EtlRequest> getCurrentOffsetRequest() throws Exception {
        Map<TopicPartition, EtlRequest> map = new HashMap<>();
        List<EtlRequest> previousList = getPreviousFromLocal();
        Map<TopicPartition, EtlRequest> previous = new HashMap<>();
        if (previousList != null) {
            previous.putAll(convertToMap(previousList));
        }
        //   LOG.info("previous offset is " + previous);
        KafkaCluster kc = KafkaCluster.apply(this.kafkaParams);
        Collection<TopicAndPartition> parts = kc.getPartitions(this.topics);
        //   LOG.info("TopicAndPartition " + parts);
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> latestLeaderOffsets = getLatestLeaderOffsets(parts, kc);

        long startOffsetSum = 0l;
        long endOffsetSum = 0l;
        for (Map.Entry<TopicAndPartition, KafkaCluster.LeaderOffset> entry : latestLeaderOffsets.entrySet()) {
            TopicAndPartition pp = entry.getKey();
            String topic = pp.topic();
            int partition = pp.partition();
            long endOffset = entry.getValue().offset();
            TopicPartition tp = new TopicPartition(topic, partition);
            EtlRequest topPrevious = previous.get(tp);
            //     LOG.info("previous :" + topPrevious);
            long startOffset = -1l;

            if (topPrevious == null) {
                startOffset = endOffset;
                //      LOG.info("previous is null :" + topPrevious);
            } else {
                startOffset = topPrevious.getEndOffset();
            }
            startOffsetSum += startOffset;
            endOffsetSum += endOffset;
            //startOffset,作为此次request的startOffset，latestOffset作为endOffset
            EtlRequest topCurrent = new EtlRequest(tp, startOffset, endOffset);
            //     LOG.info("current :" + topCurrent);
            map.put(tp, topCurrent);
        }
        LOG.info("start offset sum number is " + startOffsetSum);
        LOG.info("end offset sum number is " + endOffsetSum);
        LOG.info("total count is " + (endOffsetSum - startOffsetSum));
        LOG.info("make current request finish");

        return map;
    }

    public Map<TopicAndPartition, KafkaCluster.LeaderOffset> getLatestLeaderOffsets(Collection<TopicAndPartition> parts, KafkaCluster kc) throws InterruptedException {
        Map<TopicAndPartition, KafkaCluster.LeaderOffset> latestLeaderOffsets = null;
        int i = 0;
        int tryNumber = 30;
        while (i != tryNumber) {
            try {
                latestLeaderOffsets = kc.getLatestLeaderOffsets(parts);
                i = tryNumber;
            } catch (Exception e) {
                e.printStackTrace();
                i++;
                Thread.sleep(5000);
            }
        }
        if (latestLeaderOffsets == null) {
            throw new RuntimeException("get partitions latest offset infomation from topics error.");
        }
        return latestLeaderOffsets;
    }

    public void run() throws Exception {
        String pathLocal = FileUtil.pathPreProcess(this.offsetRecorderPathLocal) + this.time + File.separator + FILE_NAME;
        Map<TopicPartition, EtlRequest> map = getCurrentOffsetRequest();

        LOG.info("current request information save to file :" + pathLocal);
        writeCurrentRequestLocal(map, pathLocal);
        LOG.info(this.time + " request finish.");

    }

    private void writeCurrentRequestLocal(Map<TopicPartition, EtlRequest> map, String outPath) {
        List<String> strs = new ArrayList<>();
        for (TopicPartition key : map.keySet()) {
            strs.add(map.get(key).toString());
        }
        FileUtil.listToFile(outPath, strs);

    }

    private void writeCurrentRequest(Configuration conf, Map<TopicPartition, EtlRequest> map, String outPath) throws IOException {
        List<String> strs = new ArrayList<>();
        for (TopicPartition key : map.keySet()) {
            strs.add(map.get(key).toString());
        }
        FileSystem fs = FileSystem.get(conf);
        FileUtil.writeListToHdfsFile(fs, new Path(outPath), strs);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("OffsetRecorder <time> <lastTime> <offsetRecorderPath> <topics> <brokerList> <offsetRecorderPathLocal>");
            System.exit(1);
        }
        long time = Long.valueOf(args[0]);
        long lastTime = Long.valueOf(args[1]);
        String offsetRecorderPath = args[2];
        List<String> topics = Arrays.asList(args[3].split(","));
        String brokerList = args[4];
        String offsetRecorderPathLocal = args[5];
        new OffsetRecorder(time, lastTime, offsetRecorderPath, topics, brokerList, offsetRecorderPathLocal).run();
    }
}
