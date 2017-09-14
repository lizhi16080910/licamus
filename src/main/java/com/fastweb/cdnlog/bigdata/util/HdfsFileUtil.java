package com.fastweb.cdnlog.bigdata.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lfq on 2017/4/13.
 */
public class HdfsFileUtil {

    private static final Log LOG = LogFactory.getLog(HdfsFileUtil.class);

    public static List<String> readHdfsFile(Configuration conf, Path path, boolean ifFsClose) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return readHdfsFile(fs, path, ifFsClose);
    }

    /**
     * @param fs
     * @param path
     * @return
     * @throws
     * @Title: readHdfsFile
     * @Description: 传入的fs参数并不会关闭，需要另外写命令关闭fs参数
     */
    public static List<String> readHdfsFile(FileSystem fs, Path path, boolean ifFsClose) {
        List<String> list = new ArrayList<>();
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;
        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ifFsClose) {
                    if (fs != null) {
                        fs.close();
                    }
                }
                if (reader != null) {
                    reader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static List<String> readHdfsFile(Path path) throws IOException {
        Configuration conf = new Configuration();
        return readHdfsFile(conf, path, true);
    }

    public static List<String> readHdfsFile(Configuration conf, String path) throws IOException {
        return readHdfsFile(conf, new Path(path), true);
    }

    public static List<String> readHdfsFile(String path) throws IOException {
        Configuration conf = new Configuration();
        return readHdfsFile(conf, new Path(path), true);
    }

    public static boolean ifHdfsFileExist(String path) {
        return ifHdfsFileExist(new Path(path));
    }

    public static boolean ifHdfsFileExist(Path path) {
        Configuration conf = new Configuration();
        // conf.set("fs.default.name", "hdfs://192.168.100.202:8020");
        FileSystem fs = null;
        boolean ifExist = false;
        try {
            fs = FileSystem.get(conf);
            if (fs.exists(path)) {
                ifExist = true;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            ifExist = false;
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
        return ifExist;
    }

    /**
     *
     * @param path
     * @return
     *  判断传入的路径最后一个字符是否是分隔符，不是的话，添加分隔符
     *
     *  */
    public static String hdfsPathPreProcess(String path) {
        return FileUtil.pathPreProcess(path,Path.SEPARATOR);
    }


}
