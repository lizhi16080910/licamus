package com.fastweb.cdnlog.bigdata.merge.smallfiletobig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by lfq on 2017/3/15.
 */
public class MergeSmallFileJob extends Configured implements Tool {
    public static final String JOB_TIME = "cdnlog.job.time";
    public static final String OUT_PATH = "cdnlog.output.path";

    /**
     *
     * @param jobTime  ，任务的时间
     * @param inputPath ，输入目录
     * @param domainInfoPath ，保存域名文件大小信息的路径
     * @param outputPath  ，合并后的输出目录
     * @return 返回1，表示执行成功，返回0，表示执行失败
     * @throws Exception
     */
    public static int execute(String jobTime,String inputPath,String domainInfoPath,String outputPath,String jobName)
            throws Exception {
        MergeSmallFileJob job = new MergeSmallFileJob();
        String[] args = new String[5];
        args[0] = jobTime;
        args[1] = inputPath;
        args[2] = domainInfoPath;
        args[3] = outputPath;
        args[5] = jobName;
        return ToolRunner.run(job, args);
    }

    @Override
    public int run(String[] args) throws Exception {
//        String jobTime = "2016/12/19/12";
//        String inputPath = "/user/lifq/test/2016_12_19_12";
//        String domainInfoPath = "/user/lifq/test/domain-info";
//        String outputPath = "/user/lifq/test/download/";

        String jobTime = args[0];
        String inputPath = args[1];
        String domainInfoPath = args[2];
        String outputPath = args[3];
        String jobName = args[4];

        Configuration conf = getConf();
        conf.set(JOB_TIME, jobTime);
        conf.set(OUT_PATH, outputPath);
       // conf.set("mapreduce.job.queuename", "queueA");

        Job job = Job.getInstance(getConf());

        job.setJarByClass(this.getClass());
        job.setJobName(jobName);
        job.setMapperClass(MergeMap.class);
        job.setReducerClass(MergeReduce.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(MergeFileInpuformat.class);
        MergeFileInpuformat.setInputPath(job, new Path(inputPath));

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(domainInfoPath));

        job.submit();
        if (job.waitForCompletion(true)) {
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        MergeSmallFileJob job = new MergeSmallFileJob();
        ToolRunner.run(job, args);
    }
}
