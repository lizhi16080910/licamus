package com.fastweb.cdnlog.bigdata.mapred.custom;

import com.fastweb.cdnlog.bigdata.duowan.Domains;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

/**
 * Created by lfq on 2016/9/7.
 */
public class CdnlogMapper extends Mapper<CustomKafkaInputFomat.EtlKey, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(CdnlogMapper.class);
    private static String ERROR_OUT_PATH = "error.path"; //解析失败的log输出目录

    private OutputStream out = null;
    private FileSystem fs = null;
    private Set<String> domains = null;

    private String time = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String errorPath = null;
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        int taskId = taskAttemptID.getTaskID().getId();
        int jodId = taskAttemptID.getJobID().getId();
        String rootPath = conf.get(ERROR_OUT_PATH);
        if (rootPath != null) {
            errorPath = rootPath + Path.SEPARATOR + conf.get("job_time").replace("-","/") + Path.SEPARATOR + taskAttemptID.getTaskType().name() + "-" + jodId + "-" + taskId;
        } else {
            errorPath = context.getConfiguration().get("etl.execution.base.path") + Path.SEPARATOR + "error" + Path.SEPARATOR + conf.get("job_time").replace("-","/") + Path.SEPARATOR + taskAttemptID.getTaskType().name() + "-" + jodId + "-" + taskId;
        }
        out = fs.create(new Path(errorPath));
        domains = Domains.apply();
        this.time = context.getConfiguration().get("job_time").replace("-", "/");
    }

    @Override
    protected void map(CustomKafkaInputFomat.EtlKey key, Text value, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        KeyValue kv = logTrim(value.toString()); //对字段进行修剪和过滤

        if (kv != null) {
            context.write(new Text(kv.getDomain() + Path.SEPARATOR + this.time), new Text(kv.getLog()));
        }
        long endTime = System.currentTimeMillis();
        long mapTime = ((endTime - startTime));
        context.getCounter("total", "mapper-time(ms)").increment(mapTime);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        out.close();
    }

    /**
     * @param line
     * @return KeyValue
     * 对日志的格式进行修剪，删除不必要的字段（包括，每行记录的前两个字段和最后一个字段），同时过滤掉ip为127.0.0.1的日志
     */
    public KeyValue logTrim(String line) throws IOException {
        try {
            int firstBlankIndex = line.indexOf(" ") + 1;
            int secondBlankIndex = line.indexOf(" ", firstBlankIndex) + 1;
            int lastBlankIndex = line.lastIndexOf(" ");
            String log = line.substring(secondBlankIndex, lastBlankIndex);

            int startIndex = 0;
            int endIndex = log.indexOf("#_#"); //
            if (endIndex < 0) {
                return null;
            }
            String domain = log.substring(startIndex, endIndex);//解析域名
            if (!domains.contains(domain)) {
                return null;
            }
            int ipStartIndex = endIndex + 3;
            int ipEndIndex = log.indexOf("#_#", ipStartIndex);
            String ip = log.substring(ipStartIndex, ipEndIndex); //解析IP

            //如果ip为127.0.0.1，丢弃
            if (ip.equals("127.0.0.1")) {
                out.write(log.getBytes());
                out.write("\n".getBytes());
                return null;
            }

            //去掉c06.i06平台url中的http://
            int requestIndex1 = log.indexOf("]#_#", ipEndIndex + 3);
            int requestIndexEnd = log.indexOf("#_#", requestIndex1 + 4);
            int requestIndexBlank = log.indexOf(" ",requestIndex1 + 4);
            int httpIndex = log.indexOf("://",requestIndex1);
            int domainIndex = log.indexOf("/",httpIndex + 3);
            StringBuilder strBuilder = new StringBuilder();
            if(httpIndex < requestIndexEnd){
                strBuilder.append(log.substring(0,requestIndexBlank + 1));
                strBuilder.append(log.substring(domainIndex));
                return new KeyValue(domain,strBuilder.toString());
            }
            return new KeyValue(domain, log);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.error(line);
            out.write(line.getBytes());
            out.write("\n".getBytes());
            //将解析失败的line，写入到error文件中
            return null;
        }
    }


    public static class KeyValue {
        private String domain = null;
        private String log = null;

        public KeyValue(String domain, String line) {
            this.domain = domain;
            this.log = line;
        }

        public String getDomain() {
            return domain;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        public String getLog() {
            return log;
        }

        public void setLog(String log) {
            this.log = log;
        }
    }

    public static void setErrorOutPath(Job job, String path) {
        job.getConfiguration().set(ERROR_OUT_PATH, path);
    }

    public static void main(String[] args) throws IOException {
        String log = "CDNLOG ctl--- dl.vip.yy.com#_#218.2.78.6#_#-#_#[28/Oct/2016:12:00:07 +0800]#_#GET http://dl.vip.yy.com/vipface/20131129.7z HTTP/1.1#_#200#_#0#_#http://hlahgla/alghal#_#-#_#0.000#_#0.000#_#61\n" +
                ".147.218.24#_#TCP_HIT -#_#fastweb#_#380 12.0.0.0";
        System.out.println(new CdnlogMapper().logTrim(log).getLog());
        System.out.println(new CdnlogMapper().logTrim(log).getDomain());
        //System.out.println(log);
    }
}
