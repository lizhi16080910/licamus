package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.duowan.AfterJob;
import com.fastweb.cdnlog.bigdata.inputformat.kafka.KafkaInputFomat;
import com.fastweb.cdnlog.bigdata.mapred.Constant;
import com.fastweb.cdnlog.bigdata.mapred.multiout.MultiOutputFormat;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import com.fastweb.cdnlog.bigdata.util.SendMailUtil;
import com.hadoop.compression.lzo.DistributedLzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by lfq on 2016/10/13.
 */
public class ClassifyByTimeJob extends Configured implements Tool {
    public static final String DEFAULT_JOB_NAME_PREFIX = "camus_job";
    public static final String JOB_TIME = "job_time";
    public static final String FINAL_RESULT_PATH = "final.result.path";
    public static final String CDNLOG_MSG_DIR = "cdnlog.msg.dir";
    public static final String CDNLOG_MSG_START = "cdnlog.msg.start";
    public static final String IO_STATISTIC_PATH = "io.statistic.path";
    public static final String CDNLOG_TIMEOUT_MUCH ="cdnlog.timeout.much";

    public static Properties props;

    public static void main(String[] args) throws Exception {
        ClassifyByTimeJob job = new ClassifyByTimeJob();
        ToolRunner.run(job, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage:KafkaJob <property file>");
            System.exit(1);
        }

        String propertyFile = args[0];

        Properties props = loadProperty(propertyFile);
        populateConf(props, getConf());

        Configuration conf = getConf();

        String offsetDir = FileUtil.hdfsPathPreProcess(getConf().get(com.fastweb.cdnlog.bigdata.duowan.Constant.ETL_EXECUTION_OFFSET_PATH));
        FileSystem fs = FileSystem.get(getConf());
        Path offsetDirP = new Path(offsetDir);

        FileStatus[] fsts = fs.listStatus(offsetDirP);
        if (fsts.length == 0) {
            return 1;
        }
        List<String> dates = new ArrayList<>();
        for (FileStatus fst : fsts) {
            dates.add(fst.getPath().getName());
        }
        Collections.sort(dates);
        conf.set(JOB_TIME, dates.get(0));
        String jobTime = dates.get(0);

        System.out.println(conf.get(Constant.KAFKA_WHITELIST_TOPIC));

        Job job = createJob(props);

        //判断超时日志是否很多，如果很多的话就采用超时日志分散到多个文件的类 ，1:表示超时日志很多；0：表示超时日志很少
        if(getConf().get(CDNLOG_TIMEOUT_MUCH).equals("1")){
            job.setMapperClass(ExtraTimeTimeoutMapper.class);
        }else {
            job.setMapperClass(ExtraTimeMapper.class);
        }

        job.setReducerClass(CdnlogReducer.class);

        job.setInputFormatClass(KafkaInputFomat.class);
        job.setOutputFormatClass(OutputFormatByTime.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置kafka  offset 信息的文件
        String offsetPath = FileUtil.hdfsPathPreProcess(offsetDir) + jobTime + Path.SEPARATOR + KafkaInputFomat.FILE_NAME;
        KafkaInputFomat.setOffsetInputPath(offsetPath);

        String outPath = FileUtil.hdfsPathPreProcess(getConf().get(Constant.ETL_DESTINATION_PATH)) + dates.get(0);
        //mr运算结果的输出目录设置
        MultiOutputFormat.setOutputPath(job, new Path(outPath));

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        job.submit();
        if (job.waitForCompletion(true)) {
            saveIOStatistic(job);
            fs.delete(new Path(offsetDir + dates.get(0)), true);
//            LzoIndexer lzoIndexer = new LzoIndexer(conf);  //单机生成索引
//            lzoIndexer.index(new Path(outPath));
            String[] p = new String[]{outPath};
            int exitCode = ToolRunner.run(new DistributedLzoIndexer(), p);//采用分布式方法生成lzo索引
            if(exitCode == 1){ //如果生成失败，重新生成一次
                ToolRunner.run(new DistributedLzoIndexer(), p);
            }

            boolean state = AfterJob.moveFile(FileUtil.hdfsPathPreProcess(outPath), FileUtil.hdfsPathPreProcess
                            (getConf().get(FINAL_RESULT_PATH)),
                    getConf());
            if (state) {
                fs.delete(new Path(outPath), true);
            }
            if (dates.get(0).substring(14).equals(getConf().get(CDNLOG_MSG_START))) {
                sendLogMergeStartMsg(FileUtil.pathPreProcess(getConf().get(CDNLOG_MSG_DIR)));
            }
        } else {
            fs.delete(new Path(outPath), true);
            sendJobFailWarnMsg(job);
        }
        fs.close();
        return 0;
    }

    private void sendJobFailWarnMsg(Job job) {
        String fromMail = getConf().get("fromMail");
        String user = getConf().get("user");
        String pwd = getConf().get("pwd");
        String toMail = getConf().get("toMail");
        String title = getConf().get("title");
        StringBuilder mailContent = new StringBuilder("");
        String smtpHost = getConf().get("smtpHost");
        mailContent.append("MR Job " + job.getJobName() + " run failedly ! please check.");

        File attachmentFile = null;
        try {
            SendMailUtil.sendMail(fromMail, user, pwd, toMail, title, mailContent.toString(), smtpHost,
                    attachmentFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveIOStatistic(Job job) throws IOException {
        Counters c = job.getCounters();

        long inputCount = c.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        long outputCount = c.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
        long errorLogCount = c.findCounter("total-error-records", "error-log").getValue();
        List<String> result = new ArrayList<>();
        result.add("inputCount=" + inputCount);
        result.add("outputCount=" + outputCount);
        result.add("errorLogCount=" + errorLogCount);
        FileUtil.listToFile(FileUtil.pathPreProcess(getConf().get(IO_STATISTIC_PATH)) + getConf().get(JOB_TIME)
                .replace("-", "/"), result);
    }

    private boolean sendLogMergeStartMsg(String path) throws Exception {
        SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        String hour = sdf.format(new Date(sdf3.parse(getConf().get(JOB_TIME)).getTime() - 3600 * 1000l));
        File file = new File(FileUtil.pathPreProcess(path) + hour);
        return file.createNewFile();
    }

    private Job createJob(Properties props) throws IOException {
        Job job;
        if (getConf() == null) {
            setConf(new Configuration());
        }
        populateConf(props, getConf());

        job = Job.getInstance(getConf());
        job.setJarByClass(this.getClass());

        if (job.getConfiguration().get(Constant.CAMUS_JOB_NAME) != null) {
            job.setJobName(job.getConfiguration().get(Constant.CAMUS_JOB_NAME) + "-" + getConf().get("job_time"));
        } else {
            job.setJobName(DEFAULT_JOB_NAME_PREFIX);
        }
        return job;
    }

    public static Properties loadProperty(String file) throws Exception {
        Properties props = new Properties();
        InputStream fStream;
        if (file.startsWith("hdfs:")) {
            Path pt = new Path(file);
            FileSystem fs = FileSystem.get(new Configuration());
            fStream = fs.open(pt);
        } else {
            File file2 = new File(file);
            fStream = new FileInputStream(file2);
        }
        props.load(fStream);
        fStream.close();
        return props;
    }

    private void populateConf(Properties props, Configuration conf) throws IOException {
        for (Object key : props.keySet()) {
            conf.set(key.toString(), props.getProperty(key.toString()));
        }
    }

}
