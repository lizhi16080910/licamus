package com.fastweb.cdnlog.bigdata.mapred.custom;

import com.fastweb.cdnlog.bigdata.mapred.Constant;
import com.fastweb.cdnlog.bigdata.mapred.multiout.MultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

/**
 * Created by lfq on 2016/10/13.
 */
public class KafkaJobNoreduce extends Configured implements Tool {
    public static Properties props;

    public static void main(String[] args) throws Exception {
        KafkaJobNoreduce job = new KafkaJobNoreduce();
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

        //   DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss", DateTimeZone.forOffsetHours(8));
        //    String executionDate = new DateTime().toString(dateFmt);
        Configuration conf = getConf();

        String historyDir = conf.get(Constant.ETL_EXECUTION_HISTORY_PATH);
        FileSystem fs = FileSystem.get(conf);
        Path historyDirP = new Path(historyDir);

        FileStatus[] fsts = fs.listStatus(historyDirP);
        if (fsts.length == 0) {
            return 1;
        }
        List<String> dates = new ArrayList<>();
        for (FileStatus fst : fsts) {
            dates.add(fst.getPath().getName());
        }
        Collections.sort(dates);
        conf.set("job_time", dates.get(0));

        System.out.println(conf.get(Constant.KAFKA_WHITELIST_TOPIC));

        Job job = createJob(props);
        job.setJobName("Camus_Job-" + dates.get(0));
        job.setMapperClass(CdnlogMapper.class);
        //  job.setReducerClass(CdnlogReducer.class);

        //job.setNumReduceTasks(30);
        job.setInputFormatClass(CustomKafkaInputFomat.class);
        job.setOutputFormatClass(MultiOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String outPath = "/user/camus-test/result/" + dates.get(0).replace("-", Path.SEPARATOR);
        //mr运算结果的输出目录设置
        MultiOutputFormat.setOutputPath(job, new Path(outPath));
        //FileOutputFormat.setOutputPath(job, new Path("/user/camus-test/result/" + executionDate));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        job.submit();
        if (job.waitForCompletion(true)) {
            fs.delete(new Path(historyDir + Path.SEPARATOR + dates.get(0)), true);
            moveFile(outPath, "/download/fastweb_vip/", getConf());
        }
        fs.delete(new Path(outPath), true);

        return 0;
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
            job.setJobName(job.getConfiguration().get(Constant.CAMUS_JOB_NAME));
        } else {
            job.setJobName("Camus_Job");
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

    public static void moveFile(Path srcPath, Path destPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> paths = fs.listFiles(srcPath, true);
        while (paths.hasNext()) {
            Path src = paths.next().getPath();
            if (src.getName().equals("_SUCCESS")) {
                continue;
            }
            String srcString = src.toString();
            String destString = srcString.replace(srcPath.toString(), destPath.toString());
            fs.mkdirs(new Path(destString).getParent());
            fs.rename(new Path(srcString), new Path(destString));
        }
        fs.close();
    }

    public static void moveFile(String srcPath, String destPath, Configuration conf) throws IOException {
        moveFile(new Path(srcPath), new Path(destPath), conf);
    }
}
