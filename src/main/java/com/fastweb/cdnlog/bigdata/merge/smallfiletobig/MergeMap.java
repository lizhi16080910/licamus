package com.fastweb.cdnlog.bigdata.merge.smallfiletobig;

import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by lfq on 2017/3/15.
 */
public class MergeMap extends Mapper<NullWritable, Text, NullWritable, Text> {

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String jobTime = conf.get(MergeSmallFileJob.JOB_TIME);
        String inputPath = FileUtil.hdfsPathPreProcess(conf.get(MergeFileInpuformat.MERGE_INPUT_PATH));
        String outputPath = FileUtil.hdfsPathPreProcess(conf.get(MergeSmallFileJob.OUT_PATH));
        String domains = value.toString();
        String[] domainList = domains.split(",");

        for (String domain : domainList) {
            StringBuilder result = new StringBuilder();
            String dir = inputPath + domain + Path.SEPARATOR + jobTime.substring(0,10);
            String dest = outputPath + domain + Path.SEPARATOR + jobTime + ".gz";
            Path destPath = new Path(dest);
            FileStatus[] fsts = fs.listStatus(new Path(dir));
            if (fsts.length == 1) {
                Path path = fsts[0].getPath();
                boolean stat = fs.mkdirs(destPath.getParent());
                if(!stat){
                    stat = fs.mkdirs(destPath.getParent());
                }
                if(stat){
                    boolean state = fs.rename(path, destPath);
                    if(!state){
                        fs.rename(path, destPath);
                    }
                }
            } else {
                OutputStream outputStream = fs.create(destPath);
                for (FileStatus fst : fsts) {
                    InputStream inputStream = fs.open(fst.getPath());
                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
                    inputStream.close();
                }
                outputStream.close();
            }
            result.append(domain + "," + getFileSize(fs, destPath) + "," + jobTime);
            context.write(NullWritable.get(), new Text(result.toString()));
        }

    }

    public long getFileSize(FileSystem fs, Path path) throws IOException {
        FileStatus fst = fs.getFileStatus(path);
        return fst.getLen();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //new MergeMap().map();
        String str = "2016/12/19/12";
        System.out.println(str.substring(0,10));

        String domain = "s.cimg.163.com";
        System.out.println(domain.split(",").length);
    }
}
