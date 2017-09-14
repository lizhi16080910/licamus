package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.mapred.multiout.MultiOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.NumberFormat;

/**
 * Created by lfq on 2016/11/3.
 */
public class OutputFormatByTime<K, V> extends MultiOutputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(OutputFormatByTime.class);
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    @Override
    protected String generateFileNameForKeyValue(K key, V value, TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        StringBuilder result = new StringBuilder();
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        int taskId = taskAttemptID.getTaskID().getId();

        result.append(conf.get(FileOutputFormat.OUTDIR) + Path.SEPARATOR + key.toString())
                .append("-")
                .append(conf.get(ClassifyByTimeJob.JOB_TIME))
                .append("-m")
                .append("-" + NUMBER_FORMAT.format(taskId));
//        LOG.info(key.getClass());
//        LOG.info("output dir file name is :" + result);

        return result.toString();
    }
}
