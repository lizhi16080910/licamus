package com.fastweb.cdnlog.bigdata.duowan.together;

import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import com.fastweb.cdnlog.bigdata.util.UrlUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by lfq on 2016/11/10.
 */
public class DomainReportMix {

    private String fcname = null;
    private String fckey = null;
    private String url = null;
    private String logType = null;
    private String logNode = null;
    private Properties props = null;
    private String domainInfoDir = null;
    private int pushEnable = 0;
    private int pushEdgeEnable = 0;
    private int pushParentEnable = 0;
    private int pushSuperEnable = 0;

    /**
     * @param path 配置文件的路径
     * @throws Exception
     */
    public DomainReportMix(String path) throws Exception {
        props = loadProperty(path);
        this.fcname = props.getProperty(Constant.CDNLOG_MERGE_DOMAIN_POST_FCNAME);
        this.fckey = props.getProperty(Constant.CDNLOG_MERGE_DOMAIN_POST_FCKEY);
        this.url = props.getProperty(Constant.CDNLOG_MERGE_DOMAIN_POST_URL);
        this.logType = props.getProperty(Constant.CDNLOG_MERGE_DOMAIN_LOGTYPE);
        this.logNode = props.getProperty(Constant.CDNLOG_MERGE_DOMAIN_SOURCE);
        this.pushEnable = Integer.valueOf(props.getProperty(Constant.DOMAIN_INFO_PUSH_ENABLE, "0"));
        this.pushEdgeEnable = Integer.valueOf(props.getProperty(Constant.DOMAIN_INFO_PUSH_EDGE_ENABLE, "0"));
        this.pushParentEnable = Integer.valueOf(props.getProperty(Constant.DOMAIN_INFO_PUSH_PARENT_ENABLE, "0"));
        this.pushSuperEnable = Integer.valueOf(props.getProperty(Constant.DOMAIN_INFO_PUSH_SUPER_ENABLE, "0"));
        this.domainInfoDir = props.getProperty(Constant.DOMAIN_INFO_PUSH_TEMP_DIR);
    }

    /**
     * 读取path目录下的所有文件，获得domain信息，推送给cs，供客户下载日志
     */
    public void reportDomainLogInfo() throws Exception {
        if (this.pushEnable == 0) {
            log("push message is forbiden.");
            return;
        }

        log("start to push message.");
        if (this.pushEdgeEnable != 0) {
            reportDomainLogInfo(ConstantMix.EDGE);
            log("edge domain information push message finished.");
        }

        if (this.pushParentEnable != 0) {
            reportDomainLogInfo(ConstantMix.PARENT);
            log("parent domain information push message finished.");
        }

        if (this.pushSuperEnable != 0) {
            reportDomainLogInfo(ConstantMix.SUPER);
            log("super domain information push message finished.");
        }
    }

    private void reportDomainLogInfo(String logNode) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        File dir = new File(FileUtil.pathPreProcess(this.domainInfoDir) + logNode);
        for (File file : dir.listFiles()) {
            String date = file.getName();
            String ts = String.valueOf(sdf.parse(date).getTime() / 1000);
            List<String> domainsInfo = FileUtil.readFile(file);
            if (domainsInfo.size() == 0) {
                continue;
            }
            boolean ifSuccess = DomainReportMix.submitDomain5minute(this.logType, logNode, domainsInfo, ts, fcname,
                    fckey, url);
            if (ifSuccess) {
                log(date + " domain infomation has been send successfully!");
                file.delete();
            } else {
                log(date + " domain infomation has been send failly!");
            }
        }
    }

    public static boolean submitDomain5minute(String logType, String logNode, List<String> domains,
                                              String ts, String fcname, String fckey, String posturl) throws Exception {

        String domainName;

        String postStr = null;
        String statusStr = null;
        String statusStrPredix = "\"status\":";
        int idx1;
        int idx2;

        String fctoken = DigestUtils.md5Hex(new SimpleDateFormat("yyyyMMdd").format(new Date(System
                .currentTimeMillis())) + DigestUtils.md5Hex(fckey));

        // 拼接字符串
        StringBuilder sb = new StringBuilder();
        sb.append("{\"time\":\"");
        sb.append(ts);
        sb.append("\"," + "\"log_type\":\"");
        sb.append(logType);
        sb.append("\"," + "\"log_node\":\"");
        sb.append(logNode);
        sb.append("\",\"domain\":[");

        Iterator<String> iterator = domains.iterator();
        while (iterator.hasNext()) {
            domainName = iterator.next();
            sb.append("\"").append(StringEscapeUtils.escapeJson(domainName)).append("\"");
            sb.append(",");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append("],");
        sb.append("\"fcname\":").append("\"").append(fcname).append("\",");
        sb.append("\"fctoken\":").append("\"").append(fctoken).append("\"");
        sb.append("}");
        postStr = sb.toString();
        // LOG.info(postStr + "\n");
        log(postStr + "\n");
        int tryNumber = 5;
        int tryCount = 0;
        while (tryCount != tryNumber) {
            statusStr = "";
            String result = UrlUtil.postData(posturl, postStr);
            // LOG.info(result);
            log(result);
            idx1 = result.indexOf(statusStrPredix);
            if (idx1 != -1) {
                idx2 = result.indexOf(",");
                if (idx2 != -1) {
                    statusStr = result.substring(idx1 + statusStrPredix.length(), idx2);
                    // LOG.info("statusStr = " + statusStr + "\n");
                    log("statusStr = " + statusStr + "\n");
                }
            }
            if (statusStr.length() == 0 || statusStr.equals("0")) // failed
            {
                tryCount++;
                log("try count is :" + (tryNumber - tryCount));
                Thread.sleep(2000);
            } else {
                return true;
            }
        }
        return false;
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

    public static void log(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println(sdf.format(new Date()) + ": " + str);
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("error");
            System.out.println("DomainReport <conFile>");
        }

        String confFile = args[0];
        new DomainReportMix(confFile).reportDomainLogInfo();
    }
}
