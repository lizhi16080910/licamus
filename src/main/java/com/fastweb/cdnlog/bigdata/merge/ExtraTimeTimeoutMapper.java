package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.inputformat.kafka.EtlKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by lfq on 2016/11/3.
 */
public class ExtraTimeTimeoutMapper extends Mapper<EtlKey, Text, Text, Text> {
    public static final String TIME_OUT = "timeout";
    public static final String ERROR = "error";
    private SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH");
    private SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    private int taskId = -1;
    private int id = -1;
    private long jobHour = -1l;
    private long jobTime = -1l;
    private String day = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        this.taskId = taskAttemptID.getTaskID().getId();
        id = (this.taskId + 4) / 4;
        try {
            jobTime = sdf3.parse(context.getConfiguration().get("job_time")).getTime() / 1000l;
            jobHour = jobTime - jobTime % 3600;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        day = context.getConfiguration().get("job_time").substring(0, 10).replace("-", "/");
    }

    @Override
    protected void map(EtlKey key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String log = value.toString();
            int index1 = log.indexOf("[");
            if (index1 < 0) {
             //   context.write(new Text(ERROR),value);
                return;
            }
            int index2 = log.indexOf("]");
            if (index2 < index1) {
                context.write(new Text(ERROR),value);
                return;
            }

            String time = log.substring(index1 + 1, index2);
            long timeL = sdf.parse(time).getTime();
            String hour = sdf2.format(timeL);
            // 或者4200 jobtTime 并不是实际的MR运行时间，而是按照时间划分的任务标识
            long timeLM = timeL / 1000l; //转换成秒数
            long timeHour = timeLM - timeLM % 3600;
            if (jobHour == timeHour) {
                context.write(new Text(hour + Path.SEPARATOR + this.id), value);
            } else if (jobHour < timeHour) {
                context.write(new Text(hour + Path.SEPARATOR + this.id), value);
            } else {
                if ((jobTime - timeHour) >= 4200) {
                    context.write(new Text(TIME_OUT + Path.SEPARATOR + day + Path.SEPARATOR + this.id), value);
                } else {
                    context.write(new Text(hour + Path.SEPARATOR + 0), value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            context.write(new Text(ERROR),value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
       //context.getCounter("total-error-records", "error-log").increment(errorLogCount);
    }

    public static void main(String[] args) throws Exception {
        String tim = "2016-11-25-10-50";
        System.out.println(tim.substring(14).replace("-", "/"));

        Map<Long, Long> map = new HashMap<>();
        System.out.println(map.getClass());

        SimpleDateFormat sdff = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        SimpleDateFormat sdff2 = new SimpleDateFormat("yyyy-MM-dd-HH");
        String hour = sdff2.format(new Date(sdff.parse("2016-11-29-13-20").getTime() - 3600 * 1000l));
        System.out.println(hour);

        long timeL = 1480395109l;
        System.out.println(timeL - timeL % 3600);
        String hour2 = sdff.format(new Date((timeL - timeL % 3600) * 1000l));
        System.out.println(hour2);
    }
}
