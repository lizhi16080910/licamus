package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.duowan.Constant;
import com.fastweb.cdnlog.bigdata.util.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by lfq on 2016/11/25.
 */
public class CdnlogReducer extends Reducer<Text, Text, Text, Text> {
    private static final Log LOG = LogFactory.getLog(CdnlogReducer.class);
    public static byte[] newLineBytes = "\n".getBytes();
    private OutputStream out = null;
    private FileSystem fs = null;
    private String errorPath = null;
    private String time = null;
    private long errorLogNumber = 0l;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.time = conf.get(Constant.JOB_TIME).replace("-", "/");
        this.fs = FileSystem.get(conf);
        TaskAttemptID taskAttemptID = context.getTaskAttemptID();
        int taskId = taskAttemptID.getTaskID().getId();
        int jodId = taskAttemptID.getJobID().getId();
        String rootPath = conf.get(Constant.ETL_EXECUTION_ERROR_PATH);
        if (rootPath != null) {
            this.errorPath = FileUtil.hdfsPathPreProcess(rootPath) + this.time
                    + Path.SEPARATOR + taskAttemptID.getTaskType().name() + "-" + jodId + "-" + taskId;
        } else {
            this.errorPath = FileUtil.hdfsPathPreProcess(context.getConfiguration().get(Constant
                    .ETL_EXECUTION_BASE_PATH))
                    + "error" + Path.SEPARATOR + this.time
                    + Path.SEPARATOR + taskAttemptID.getTaskType().name() + "-" + jodId + "-" + taskId;
        }
        System.out.println("error path is :" + this.errorPath);
        LOG.info("error path is :" + this.errorPath);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.getCounter("total-error-records", "error-log").increment(errorLogNumber);
        if(this.out != null){
            this.out.close();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.toString().equals(ExtraTimeMapper.ERROR)){
             if(this.out == null){
                 this.out = fs.create(new Path(this.errorPath));
             }
            for (Text text : values) {
                this.out.write(text.toString().getBytes());
                this.out.write(newLineBytes);
                errorLogNumber += 1l;
            }
        }else{
            for (Text text : values) {
                context.write(key, text);
            }
        }

    }
}
