package com.fastweb.cdnlog.bigdata.mapred.custom;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by lfq on 2016/10/28.
 */
public class CopyFile {
    public static void main(String[] args) throws IOException {
        String srcPath = args[0];
        String destPath = args[1];
        Configuration conf = new Configuration();
        KafkaJob.moveFile(srcPath,destPath,conf);
    }
}
