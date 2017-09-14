package com.fastweb.cdnlog.bigdata.mapred.custom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lfq on 2017/1/16.
 */
public class OffsetRecorderRepair {
    private static final Log LOG = LogFactory.getLog(OffsetRecorderRepair.class);

    private String startPath = null;
    private String endPath = null;
    private String outDir = null;
    private String time = null;

    public OffsetRecorderRepair(String time, String startPath, String endPath, String outDir) {
        this.time = time;
        this.endPath = endPath;
        this.startPath = startPath;
        this.outDir = outDir;
    }

    public OffsetRecorderRepair(long time, String startPath, String endPath, String outDir) {
        this.time = OffsetRecorder.sdf.format(new Date(time * 1000));
        this.endPath = endPath;
        this.startPath = startPath;
        this.outDir = outDir;
    }

    public void run() throws Exception {
        String pathLocal = this.outDir + File.separator + this.time + File.separator + OffsetRecorder.FILE_NAME;
        LOG.info("current request information save to file :" + pathLocal);
        OffsetRecorder.writeCurrentRequestLocal(getCurrentOffsetRequest(), pathLocal);
    }

    private Map<CustomKafkaInputFomat.TopicPartition, CustomKafkaInputFomat.EtlRequest> getCurrentOffsetRequest() throws Exception {
        Map<CustomKafkaInputFomat.TopicPartition, CustomKafkaInputFomat.EtlRequest> map = new HashMap<>();
        List<CustomKafkaInputFomat.EtlRequest> startOffsetsList = getStartOffsets();
        Map<CustomKafkaInputFomat.TopicPartition, CustomKafkaInputFomat.EtlRequest> startOffsetsMap = OffsetRecorder
                .convertToMap(startOffsetsList);
        List<CustomKafkaInputFomat.EtlRequest> endOffsetsList = getEndOffsets();
        long startOffsetSum = 0l;
        long endOffsetSum = 0l;
        for (CustomKafkaInputFomat.EtlRequest request : endOffsetsList) {
            String topic = request.getTopic();
            int partition = request.getPartiton();
            CustomKafkaInputFomat.TopicPartition tp = new CustomKafkaInputFomat.TopicPartition(topic, partition);

            long startOffset = startOffsetsMap.get(tp).getEndOffset();
            long endOffset = request.getStartOffset();
            CustomKafkaInputFomat.EtlRequest topCurrent = new CustomKafkaInputFomat.EtlRequest(tp, startOffset, endOffset);
            map.put(tp, topCurrent);
            startOffsetSum += startOffset;
            endOffsetSum += endOffset;
        }
        LOG.info("start offset sum number is " + startOffsetSum);
        LOG.info("end offset sum number is " + endOffsetSum);
        LOG.info("total count is " + (endOffsetSum - startOffsetSum));
        LOG.info("make current request finish");
        return map;
    }

    public List<CustomKafkaInputFomat.EtlRequest> getEndOffsets() {
        return OffsetRecorder.getPreviousFromLocal(this.endPath);
    }

    public List<CustomKafkaInputFomat.EtlRequest> getStartOffsets() {
        return OffsetRecorder.getPreviousFromLocal(this.startPath);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.out.println("OffsetRecorder <time> <startOffsetPath> <endOffsetPath> <output dir>");
            System.exit(1);
        }
        String time = args[0];
        String startOffsetPath = args[1];
        String endOffsetPath = args[2];
        String outputDir = args[3];
        new OffsetRecorderRepair(time, startOffsetPath, endOffsetPath, outputDir).run();

    }
}
