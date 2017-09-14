package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.inputformat.kafka.EtlKey;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Created by lfq on 2016/11/3.
 */
public class ExtraTimeHttpsMapper extends Mapper<EtlKey, Text, Text, Text> {
    public static final String TIME_OUT = "timeout";
    public static final String ERROR = "error";
    public static final String HTTPS = "https";
    private SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH");
    private SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    private int taskId = -1;
    private int id = -1;
    private long jobHour = -1l;
    private long jobTime = -1l;
    private String day = null;
    private Map<String, Integer> ifHttpsSeparate = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        this.taskId = taskAttemptID.getTaskID().getId();
        id = (this.taskId + 4) / 4;
        try {
            jobTime = sdf3.parse(conf.get("job_time")).getTime() / 1000l;
            jobHour = jobTime - jobTime % 3600;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        day = context.getConfiguration().get("job_time").substring(0, 10).replace("-", "/");
        FileSystem fs = FileSystem.get(conf);
        List<String> httpConf = FileUtil.readFile("https.conf");
        ifHttpsSeparate = new HashMap<>();
        for (String line : httpConf) {
            if (line.startsWith("#")) {
                continue;
            }
            ifHttpsSeparate.put(line.trim(), 1);
        }
    }

    @Override
    protected void map(EtlKey key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String log = value.toString();
            int index1 = log.indexOf("[");
            if (index1 < 0) {
                context.write(new Text(ERROR), value);
                return;
            }
            int index2 = log.indexOf("]");
            if (index2 < index1) {
                context.write(new Text(ERROR), value);
                return;
            }

            String time = log.substring(index1 + 1, index2);
            long timeL = sdf.parse(time).getTime();
            String hour = sdf2.format(timeL);

            int domainIndex1 = log.indexOf("://", index2) + 3;
            int domainIndex2 = log.indexOf("/", domainIndex1);
            String domain = log.substring(domainIndex1, domainIndex2);
            String schema = log.substring(domainIndex1 - 8, domainIndex1 - 3);
            //Integer ifSeparate = ifHttpsSeparate.get(domain);
            if (schema.equals(HTTPS) && ifHttpsSeparate.containsKey(domain)) {
                // 或者4200 jobtTime 并不是实际的MR运行时间，而是按照时间划分的任务标识
                long timeLM = timeL / 1000l; //转换成秒数
                long timeHour = timeLM - timeLM % 3600;
                if (jobHour == timeHour) {
                    context.write(new Text(HTTPS + Path.SEPARATOR + hour + Path.SEPARATOR + 0), value);
                } else if (jobHour < timeHour) {
                    context.write(new Text(HTTPS + Path.SEPARATOR + hour + Path.SEPARATOR + 0), value);
                } else {
                    if ((jobTime - timeHour) >= 4200) {
                        context.write(new Text(HTTPS + Path.SEPARATOR + TIME_OUT + Path.SEPARATOR + day + Path.SEPARATOR + 0), value);
                    } else {
                        context.write(new Text(HTTPS + Path.SEPARATOR + hour + Path.SEPARATOR + 0), value);
                    }
                }
            } else {
                // 或者4200 jobtTime 并不是实际的MR运行时间，而是按照时间划分的任务标识
                long timeLM = timeL / 1000l; //转换成秒数
                long timeHour = timeLM - timeLM % 3600;
                if (jobHour == timeHour) {
                    context.write(new Text(hour + Path.SEPARATOR + this.id), value);
                } else if (jobHour < timeHour) {
                    context.write(new Text(hour + Path.SEPARATOR + 0), value);
                } else {
                    if ((jobTime - timeHour) >= 4200) {
                        context.write(new Text(TIME_OUT + Path.SEPARATOR + day + Path.SEPARATOR + 0), value);
                    } else {
                        context.write(new Text(hour + Path.SEPARATOR + 0), value);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            context.write(new Text(ERROR), value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        //context.getCounter("total-error-records", "error-log").increment(errorLogCount);
    }

    public static void main(String[] args) throws Exception {
        String log1 = "183.240.25.87 0.000 - [20/Feb/2017:04:05:57 +0800] \"GET http://p0.pstatp" +
                ".com/origin/3792/5112637127 HTTP/1.1\" 200 13707 \"-\" \"Mozilla/5.0 (Linux; U; Android 5\n" +
                ".1; zh-CN; GM Build/LMY47D) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/40.0.2214.89 UCBrowser/11.3.8.909 Mobile Safari/537.36\" FCACHE_HIT_MEM \n" +
                " 0.041 0.000 - - - - 0.085 0.000 0.085 \"DISK HIT from 122.192.111.143, MEM HIT from 222.186.20.58\" 222.186.20.58";
        String log = "CDNLOG_cache cnc-js-112-083-122-028 122.195.43.130 0.000 - [19/Feb/2017:20:26:53 +0800] \"GET https://crystalexpress.optimix.cn/cdn/v2/adlist/1ff1a6d6a1874050\n" +
                "bf8c10aaa4ae890b/10/3 HTTP/1.1\" 200 1163 \"-\" \"Dalvik/1.6.0 (Linux; U; Android 4.4.4; Che1-CL20 Build/Che1-CL20)\" \"-\" 112.83.122.28";

        int index2 = log.indexOf("]");

        int domainIndex1 = log.indexOf("://", index2) + 3;
        int domainIndex2 = log.indexOf("/", domainIndex1);
        String domain = log.substring(domainIndex1, domainIndex2);
        String schema = log.substring(domainIndex1 - 8, domainIndex1 - 3);
        System.out.println(schema);

//        Map<String, Integer> map = new HashMap<>();
//        map.put("a.com", 1);
//        Integer a = map.get("a.com");
//        System.out.println(a != null);
    }
}
