package com.fastweb.cdnlog.bigdata.duowan.together;

import com.fastweb.cdnlog.bigdata.duowan.AfterJob;
import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.inputformat.kafka.KafkaInputFomat;
import com.fastweb.cdnlog.bigdata.outputformat.multiout.MultiOutputFormat;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import com.fastweb.cdnlog.bigdata.util.SendMailUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/** 该 类从时间最新的哪一个任务开始运行,当待运行任务小于等于3个时，停止运行，退出
 * Created by lfq on 2016/10/13.
 */
public class CdnlogDuowanLogMergeJobLatest extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(CdnlogDuowanLogMergeJobLatest.class);

    public static final String DEFAULT_JOB_NAME_PREFIX = "camus_job";
    public static final String JOB_TIME = "job_time";

    public Properties props;

    public static void main(String[] args) throws Exception {
        CdnlogDuowanLogMergeJobLatest job = new CdnlogDuowanLogMergeJobLatest();
        ToolRunner.run(job, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("usage:CdnlogParentMergeJob <property file>");
            System.exit(1);
        }

        String propertyFile = args[0];

        props = loadProperty(propertyFile);
        populateConf(props, getConf());

        String offsetDir = FileUtil.hdfsPathPreProcess(getConf().get(Constant.ETL_EXECUTION_OFFSET_PATH));
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

        if(dates.size() <= 3){
            return 0;
        }

        Collections.sort(dates);
        String jobTime = dates.get(dates.size() - 1);

        getConf().set(JOB_TIME, jobTime);

        LOG.info(getConf().get(Constant.KAFKA_WHITELIST_TOPIC));

        Job job = createJob(props);
        job.setMapperClass(CdnlogMapper.class);
        job.setReducerClass(CdnlogReducer.class);


        job.setInputFormatClass(KafkaInputFomat.class);
        job.setOutputFormatClass(MultiOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String outPath = FileUtil.hdfsPathPreProcess(getConf().get(Constant.ETL_DESTINATION_PATH)) + jobTime;

        //设置kafka  offset 信息的文件
        String offsetPath = FileUtil.hdfsPathPreProcess(offsetDir) + jobTime + Path.SEPARATOR + KafkaInputFomat.FILE_NAME;
        KafkaInputFomat.setOffsetInputPath(offsetPath);

        //mr运算结果的输出目录设置
        MultiOutputFormat.setOutputPath(job, new Path(outPath));

        MultiOutputFormat.setCompressOutput(job, true);
        MultiOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        job.submit();
        boolean result = job.waitForCompletion(true);
        if (result) {
            fs.delete(new Path(offsetDir + jobTime), true);
            saveDomainLogInfo(outPath, fs, jobTime);
            boolean state = moveFile(outPath, fs);//如果有文件移动失败，则不删除目录
            if (state) {
                fs.delete(new Path(outPath), true);
            }
        } else {
            sendJobFailWarnMsg(job);
            fs.delete(new Path(outPath), true);
        }
        LOG.info("delete outPath directory!");
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

    private boolean moveFile(String outPath, FileSystem fs) throws IOException {
        boolean result = true;
        LOG.info("start move file");
        String edgePath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.EDGE;
        if (fs.exists(new Path(edgePath))) {
            boolean state = AfterJob.moveFile(edgePath, getConf().get(ConstantMix.CDNLOG_EDGE_DOWNLOAD_PATH), getConf
                    ());
            if (!state) {
                result = false;
            } else {
                LOG.info("edge log move file finished.");
            }
        }
        String parentPath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.PARENT;
        if (fs.exists(new Path(parentPath))) {
            boolean state = AfterJob.moveFile(parentPath, getConf().get(ConstantMix.CDNLOG_PARENT_DOWNLOAD_PATH),
                    getConf());
            if (!state) {
                result = false;
            } else {
                LOG.info("parent log move file finished.");
            }
        }

        String superPath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.SUPER;
        if (fs.exists(new Path(superPath))) {
            boolean state = AfterJob.moveFile(superPath, getConf().get(ConstantMix.CDNLOG_SUPER_DOWNLOAD_PATH),
                    getConf());
            if (!state) {
                result = false;
            } else {
                LOG.info("super parent log move file finished.");
            }

        }
        LOG.info("move file finished.");
        return result;
    }

    private void saveDomainLogInfo(String outPath, FileSystem fs, String jobTime) throws IOException {
        LOG.info("start write domain infomation .");
        String edgePath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.EDGE;
        if (fs.exists(new Path(edgePath))) {
            List<String> domainInfo = AfterJob.getDomainLogInfo(edgePath);
            LOG.info(edgePath);
            LOG.info(domainInfo);

            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_SAVE_DIR)) + ConstantMix.EDGE
                            + File.separator + jobTime.replace("-", File.separator));
            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_PUSH_TEMP_DIR))
                            + ConstantMix.EDGE + File.separator + jobTime);
            LOG.info("edeg log domain infomation write finished.");
        }

        String parentPath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.PARENT;
        if (fs.exists(new Path(parentPath))) {
            List<String> domainInfo = AfterJob.getDomainLogInfo(parentPath);
            LOG.info(parentPath);
            LOG.info(domainInfo);
            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_SAVE_DIR)) + ConstantMix.PARENT
                            + File.separator + jobTime.replace("-", File.separator));
            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_PUSH_TEMP_DIR))
                            + ConstantMix.PARENT + File.separator + jobTime);
            LOG.info("parent log domain infomation write finished.");
        }

        String superPath = FileUtil.hdfsPathPreProcess(outPath) + ConstantMix.SUPER;
        if (fs.exists(new Path(superPath))) {
            List<String> domainInfo = AfterJob.getDomainLogInfo(superPath);
            LOG.info(superPath);
            LOG.info(domainInfo);
            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_SAVE_DIR)) + ConstantMix.SUPER
                            + File.separator + jobTime.replace("-", File.separator));
            AfterJob.saveDomainLogInfo(domainInfo
                    , FileUtil.pathPreProcess(props.getProperty(Constant.DOMAIN_INFO_PUSH_TEMP_DIR))
                            + ConstantMix.SUPER + File.separator + jobTime);
            LOG.info("super parent log domain infomation write finished.");
        }
    }

    private Job createJob(Properties props) throws IOException {
        Job job;
        if (getConf() == null) {
            setConf(new Configuration());
        }

        populateConf(props, getConf());

        job = Job.getInstance(getConf());
        job.setJarByClass(this.getClass());

        String jobName = null;

        if (job.getConfiguration().get(Constant.CAMUS_JOB_NAME) != null) {
            jobName = getConf().get(Constant.CAMUS_JOB_NAME) + "-" + getConf().get(JOB_TIME);
        } else {
            jobName = DEFAULT_JOB_NAME_PREFIX + "-" + getConf().get(JOB_TIME);
        }
        job.setJobName(jobName);
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
