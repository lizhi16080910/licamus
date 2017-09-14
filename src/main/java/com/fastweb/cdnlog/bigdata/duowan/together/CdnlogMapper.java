package com.fastweb.cdnlog.bigdata.duowan.together;

import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.duowan.Domains;
import com.fastweb.cdnlog.bigdata.inputformat.kafka.EtlKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;

/**
 * Created by lfq on 2016/9/7.
 */
public class CdnlogMapper extends Mapper<EtlKey, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(CdnlogMapper.class);
    public static String ERROR = "error";
    private Set<String> domains = null;
    private String time = null;
    private long costTime = 0l;

    protected void setupTest() throws IOException, InterruptedException {
        this.domains = Domains.apply();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.time = context.getConfiguration().get(Constant.JOB_TIME).replace("-", "/");
        this.domains = Domains.apply();
    }

    @Override
    protected void map(EtlKey key, Text value, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        KeyValue kv = newProcessLogTrim(value.toString());

        if (kv != null) {
            context.write(new Text(kv.getDomain() + Path.SEPARATOR + this.time), new Text(kv.getLog()));
        } else {
            context.getCounter("total-error-records", "not-duowan-log").increment(1l);
        }
        long endTime = System.currentTimeMillis();
        long mapTime = ((endTime - startTime));
        costTime += mapTime;
    }



    protected void mapOld(EtlKey key, Text value, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        KeyValue kv = null;
        if (value.toString().startsWith("@|@")) {
            kv = logTrimNew(value.toString());
        } else {
            kv = logTrim(value.toString()); //对字段进行修剪和过滤
        }

        if (kv != null) {
            context.write(new Text(kv.getDomain() + Path.SEPARATOR + this.time), new Text(kv.getLog()));
        } else {
            context.getCounter("total-error-records", "not-duowan-log").increment(1l);
        }
        long endTime = System.currentTimeMillis();
        long mapTime = ((endTime - startTime));
        costTime += mapTime;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.getCounter("total", "mapper-time(ms)").increment(costTime);
    }

    /**
     * @param line
     * @return KeyValue
     * 对日志的格式进行修剪，删除不必要的字段（包括，每行记录的前两个字段和最后一个字段），同时过滤掉ip为127.0.0.1的日志
     */
    public KeyValue logTrim(String line) throws IOException {
        try {
            String logType = ConstantMix.EDGE;
            if (line.startsWith(ConstantMix.CDNLOG_PARENT)) {
                logType = ConstantMix.PARENT;
            } else if (line.startsWith(ConstantMix.CDNLOG_SUPER)) {
                logType = ConstantMix.SUPER;
            }
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
//                int indexDot = domain.indexOf(".");
//                if(indexDot < 0){
//                    return new KeyValue(ERROR, line);
//                }
//                String extDomain = "ext" + domain.substring(indexDot);
//                if (!domains.contains(extDomain)) {
                    return new KeyValue(ERROR, line);
//                } else {
//                    domain = extDomain;
//                }
            }
            int ipStartIndex = endIndex + 3;
            int ipEndIndex = log.indexOf("#_#", ipStartIndex);
            String ip = log.substring(ipStartIndex, ipEndIndex); //解析IP

            //如果ip为127.0.0.1，丢弃
            if (ip.endsWith("127.0.0.1")) {
                return new KeyValue(ERROR, line);
            }
            //去掉c06.i06平台url中的http://
            int requestIndex1 = log.indexOf("]#_#", ipEndIndex + 3);
            int requestIndexEnd = log.indexOf("#_#", requestIndex1 + 4);
            int requestIndexBlank = log.indexOf(" ", requestIndex1 + 4);
            int httpIndex = log.indexOf("://", requestIndex1);

            int domainIndex = log.indexOf("/", httpIndex + 3);
            StringBuilder strBuilder = new StringBuilder();

            //判断是否包含‘://’字符串，若包含，
            if (httpIndex > 0 && httpIndex < requestIndexEnd) {
                strBuilder.append(log.substring(0, requestIndexBlank + 1));
                strBuilder.append(log.substring(domainIndex));
                return new KeyValue(logType + Path.SEPARATOR + domain, strBuilder.toString());
            }
            return new KeyValue(logType + Path.SEPARATOR + domain, log);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            // LOG.error(line);
            //将解析失败的line，写入到error文件中
            return new KeyValue(ERROR, line);
        }
    }

    public KeyValue logTrimNew(String line) throws IOException {
        try {
            int index1 = line.indexOf("@|@");
            int index2 = line.indexOf("@|@", index1 + 3);
            int index3 = line.indexOf("@|@", index2 + 3);
            int index4 = line.indexOf("@|@", index3 + 3);
            int index5 = line.indexOf(" ", index4);
            String logType1 = line.substring(index4 + 3, index5);
            String logType = ConstantMix.EDGE;
            if (logType1.trim().equals(ConstantMix.CDNLOG_PARENT)) {
                logType = ConstantMix.PARENT;
            } else if (logType1.trim().equals(ConstantMix.CDNLOG_SUPER)) {
                logType = ConstantMix.SUPER;
            }
            int index6 = line.indexOf(" ", index5 + 1);
            int index7 = line.indexOf("@|@", index6 + 4);
            int lastBlankIndex = line.lastIndexOf(" ");
            String log = line.substring(index7 + 3, lastBlankIndex);

            int startIndex = 0;
            int endIndex = log.indexOf("#_#"); //
            if (endIndex < 0) {
                return null;
            }
            String domain = log.substring(startIndex, endIndex);//解析域名
            if (!domains.contains(domain)) {
//                int indexDot = domain.indexOf(".");
//                if(indexDot < 0){
//                    return new KeyValue(ERROR, line);
//                }
//                String extDomain = "ext" + domain.substring(indexDot);
//                if (!domains.contains(extDomain)) {
                    return new KeyValue(ERROR, line);
//                } else {
//                    domain = extDomain;
//                }
            }
            int ipStartIndex = endIndex + 3;
            int ipEndIndex = log.indexOf("#_#", ipStartIndex);
            String ip = log.substring(ipStartIndex, ipEndIndex); //解析IP

            //如果ip为127.0.0.1，丢弃
            if (ip.endsWith("127.0.0.1")) {
                return new KeyValue(ERROR, line);
            }

            //去掉c06.i06平台url中的http://
            int requestIndex1 = log.indexOf("]#_#", ipEndIndex + 3);
            int requestIndexEnd = log.indexOf("#_#", requestIndex1 + 4);
            int requestIndexBlank = log.indexOf(" ", requestIndex1 + 4);
            int httpIndex = log.indexOf("://", requestIndex1);

            int domainIndex = log.indexOf("/", httpIndex + 3);
            StringBuilder strBuilder = new StringBuilder();
            if (httpIndex < requestIndexEnd && httpIndex > 0) {
                strBuilder.append(log.substring(0, requestIndexBlank + 1));
                strBuilder.append(log.substring(domainIndex));
                return new KeyValue(logType + Path.SEPARATOR + domain, strBuilder.toString());
            }
            return new KeyValue(logType + Path.SEPARATOR + domain, log);

        } catch (Exception e) {
            // LOG.error(e.getMessage(), e);
            // LOG.error(line);
            //将解析失败的line，写入到error文件中
            return new KeyValue(ERROR, line);
        }
    }

    public KeyValue newProcessLogTrim(String line) throws IOException {
        try {

            int firstBlankIndex = line.indexOf(" ") + 1;
            int secondBlankIndex = line.indexOf(" ", firstBlankIndex) + 1;
            int lastBlankIndex = line.lastIndexOf(" ");
         //   String log1 = line.substring(secondBlankIndex, lastBlankIndex);

            String part1 = line.substring(0,firstBlankIndex -1);
            String part2 = line.substring(secondBlankIndex,lastBlankIndex);

           // String temp[] = line.split(" ");

            String logType = ConstantMix.EDGE;
            if (part1.trim().endsWith(ConstantMix.CDNLOG_PARENT)) {
                logType = ConstantMix.PARENT;
            } else if (part1.trim().endsWith(ConstantMix.CDNLOG_SUPER)) {
                logType = ConstantMix.SUPER;
            }

            String log = part2.trim();
            if (log.startsWith("@|@")) {
                int flagIndex1 = log.indexOf("@|@");
                int flagIndex2 = log.indexOf("@|@", flagIndex1 + 3);
                log = log.substring(flagIndex2 + 3);
            }
            int startIndex = 0;
            int endIndex = log.indexOf("#_#"); //
            if (endIndex < 0) {
                return null;
            }
            String domain = log.substring(startIndex, endIndex);//解析域名
            if (!domains.contains(domain)) {
                int indexDot = domain.indexOf(".");
                if(indexDot < 0){
                    return new KeyValue(ERROR, line);
                }
                String extDomain = "ext" + domain.substring(indexDot);
                if (!domains.contains(extDomain)) {
                    return new KeyValue(ERROR, line);
                } else {
                    domain = extDomain;
                }
            }
            int ipStartIndex = endIndex + 3;
            int ipEndIndex = log.indexOf("#_#", ipStartIndex);
            String ip = log.substring(ipStartIndex, ipEndIndex); //解析IP

            //如果ip为127.0.0.1，丢弃
            if (ip.startsWith("127")) {
                return new KeyValue(ERROR, line);
            }

            //去掉c06.i06平台url中的http://
            int requestIndex1 = log.indexOf("]#_#", ipEndIndex + 3);
            int requestIndexEnd = log.indexOf("#_#", requestIndex1 + 4);
            int requestIndexBlank = log.indexOf(" ", requestIndex1 + 4);
            int httpIndex = log.indexOf("://", requestIndex1);

            int domainIndex = log.indexOf("/", httpIndex + 3);
            StringBuilder strBuilder = new StringBuilder();

            //判断是否包含‘://’字符串，若包含，
            if (httpIndex > 0 && httpIndex < requestIndexEnd) {
                strBuilder.append(log.substring(0, requestIndexBlank + 1));
                strBuilder.append(log.substring(domainIndex));
                return new KeyValue(logType + Path.SEPARATOR + domain, strBuilder.toString());
            }
            return new KeyValue(logType + Path.SEPARATOR + domain, log);

        } catch (Exception e) {
            //LOG.error(e.getMessage(), e);
            // LOG.error(line);
            //将解析失败的line，写入到error文件中
            return new KeyValue(ERROR, line);
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

        @Override
        public String toString() {
            return "KeyValue{" +
                    "domain='" + domain + '\'' +
                    ", log='" + log + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String log = "CDNLOG_parent ctl-fj-110-080-134-158 dl.vip.yy.com#_#175.174.209.115#_#-#_#[16/Nov/2016:17:25:24 +0800]#_#GET /icons/minicard_vip_prettynum_14x_0.png HTTP/1.\n" +
                "1#_#404#_#162#_#-#_#-#_#0.051#_#51.181#_#36.250.78.158#_#TCP_MISS -#_#fastweb#_#363#_#http 110.80.134.158";
        String log2 = "CDNLOG ctl--- dl.vip.yy.com#_#218.2.78.6#_#-#_#[28/Oct/2016:12:00:07 +0800]#_#GET https://dl" +
                ".vip.yy.com/vipface/20131129.7z HTTP/1.1#_#200#_#0#_#http://hlahgla/alghal#_#-#_#0.000#_#0.000#_#61\n" +
                ".147.218.24#_#TCP_HIT -#_#fastweb#_#380 12.0.0.0";
        String log7 = "CDNLOG ctl--- dl.vip.yy.com#_#218.2.78.6#_#-#_#[28/Oct/2016:12:00:07 +0800]#_#GET https://dl" +
                ".vip.yy.com/vipface/20131129.7z HTTP/1.1#_#200#_#0#_#http://hlahgla/alghal#_#-#_#0.000#_#0.000#_#61\n" +
                ".147.218.24#_#TCP_HIT -#_#fastweb#_#380#_#https 12.0.0.0";
        String log8 = "CDNLOG ctl--- exx.bs2dl.yy.com#_#218.2.78.6#_#-#_#[28/Oct/2016:12:00:07 +0800]#_#GET " +
                "https://dl" +
                ".vip.yy.com/vipface/20131129.7z HTTP/1.1#_#200#_#0#_#http://hlahgla/alghal#_#-#_#0.000#_#0.000#_#61\n" +
                ".147.218.24#_#TCP_HIT -#_#fastweb#_#380#_#http 12.0.0.0";

        String log6 = "CDNLOG_super ctl--- dl.vip.yy.com#_#218.2.78.6#_#-#_#[28/Oct/2016:12:00:07 +0800]#_#GET " +
                "/ HTTP/1.1#_#200#_#0#_#http://hlahgla/alghal#_#-#_#0.000#_#0.000#_#61\n" +
                ".147.218.24#_#TCP_HIT -#_#fastweb#_#380 12.0.0.0";
        String log4 = "@|@c06.i06@|@68@|@CTL_Taizhou2_74_89@|@CDNLOG_cache ctl-js-061-147-218-018 @|@00000000@|@dl.vip" +
                ".yy.com#_#61.191.254.226#_#-#_#[09/Nov/2016:08:58:28 +0800]#_#GET http://dl.vip.yy" +
                ".com/vipskin/icon/32.png HTTP/1.1#_#200#_#8241#_#-#_#-#_#0.000#_#0.000#_#59.63.188.153#_#TCP_HIT " +
                "-#_#fastweb#_#8667 59.63.188.151";
        String log3 = "@|@c06.i06-|-68-|-CTL_Taizhou2_74_89@|@CDNLOG_cache ctl-js-061-147-218-018 @|@00USERID@|@dl.vip.yy.com#_#61.191.254.226#_#-#_#[09/Nov/2016:08:58:28 +0800]#_#GET /vipskin/icon/32.png HTTP/1.1#_#200#_#8241#_#-#_#-#_#0.000#_#0.000#_#59.63.188.153#_#TCP_HIT -#_#fastweb#_#8667 59.63.188.151";

        String log5 = "@|@c01.i07@|@29@|@unknown@|@CDNLOG_cache ctl-fj-218-006-023-030 @|@00001051@|@dl.vip.yy" +
                ".com#_#61.191.254.226#_#-#_#[09/Nov/2016:08:58:28 +0800]#_#GET /vipskin/icon/32.png HTTP/1.1#_#200#_#8241#_#-#_#-#_#0.000#_#0.000#_#59.63.188.153#_#TCP_HIT -#_#fastweb#_#8667 59.63.188.151";
        //  System.out.println(new CdnlogMapper().logTrim(log).getLog());
        //  System.out.println(new CdnlogMapper().logTrim(log).getDomain());
        // System.out.println(new CdnlogMapper().logTrim(log).getLog());
        //System.out.println(new CdnlogMapper().newProcessLogTrim(log8).getLog());

        CdnlogMapper cm = new CdnlogMapper();
        cm.setupTest();
        if(cm.domains.contains("ext.bs2dl.yy.com")){
            System.out.println("yes");
        }
        System.out.println(cm.domains.size());
//        System.out.println( cm.newProcessLogTrim(log8));
//        System.out.println(cm.newProcessLogTrim(log3));
//        System.out.println(cm.newProcessLogTrim(log5));
//        System.out.println(cm.newProcessLogTrim(log));
//        System.out.println(cm.newProcessLogTrim(log4));
//        System.out.println(cm.newProcessLogTrim(log6));
        //System.out.println(log);
    }
}
