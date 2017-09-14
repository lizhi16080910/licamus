package com.fastweb.cdnlog.bigdata.duowan;

import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by lfq on 2016/11/10.
 */
public class AfterJob {

    private String srcPath = null;
    private String destPath = null;
    private Properties props = null;

    public AfterJob(Properties props, String jobTime) {
        this.props = props;
        this.srcPath = props.getProperty(Constant.ETL_DESTINATION_PATH) + jobTime;
        this.destPath = props.getProperty(Constant.FINAL_RESULT_PATH);
    }

    public static boolean moveFile(Path srcPath, Path destPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> paths = fs.listFiles(srcPath, true);
        boolean ifRenameSuccess = true;
        while (paths.hasNext()) {
            Path src = paths.next().getPath();
            if (src.getName().equals("_SUCCESS")) {
                continue;
            }
            String srcString = src.toString();
            String destString = srcString.replace(srcPath.toString(), destPath.toString());

            int tryCount = 0;
            int tryCountMax = 10;
            while (tryCount < tryCountMax) {
                fs.mkdirs(new Path(destString).getParent());
                boolean state = fs.rename(new Path(srcString), new Path(destString));
                tryCount++;
                if (tryCount != 1) {
                    System.out.println("try count is " + tryCount);
                }
                if (state) {
                    break;
                } else if (tryCount == tryCountMax) {
                    System.out.println("file " + destString + " move failedly.");
                    ifRenameSuccess = false;
                    break;
                }
            }
        }
        return ifRenameSuccess;
    }

    public static boolean moveFile(String srcPath, String destPath, Configuration conf) throws IOException {
        return moveFile(new Path(srcPath), new Path(destPath), conf);
    }

    /**
     * 获取域名上报的信息
     */
    public static List<String> getDomainLogInfo(String path) throws IOException {
        List<String> domainsStatus = new ArrayList<String>();
        // 获取域名
        Configuration conf = new Configuration();
        // conf.set("fs.default.name", "hdfs://192.168.100.203:8020");
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(path);
        FileStatus[] fst = fs.listStatus(dir);
        for (FileStatus f : fst) {
            long logSize = 0l;
            String domain = f.getPath().getName();
            if (domain.startsWith("_SUCCESS")) {
                continue;
            }
            RemoteIterator<LocatedFileStatus> subDir = fs.listFiles(f.getPath(), true);
            while (subDir.hasNext()) {
                LocatedFileStatus lfs = subDir.next();
                String fileName = lfs.getPath().getName();
                if (!fileName.startsWith("_SUCCESS")) {
                    logSize = lfs.getLen();
                }
                break;
            }
            domainsStatus.add(domain + " 0 " + logSize);
        }
        return domainsStatus;
    }

    public static void saveDomainLogInfo(List<String> data, String path) {
        FileUtil.listToFile(path, data);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.100.185:8020");
        boolean st = moveFile("/download/0.assets.dajieimg.com/2016/08", "/download/0.assets.dajieimg.com/2016/08", conf);
        System.out.println(st);
    }

}
