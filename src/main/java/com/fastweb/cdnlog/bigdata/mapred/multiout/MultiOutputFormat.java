/**
 * @Title: MultipleOutputFormat.java
 * @Package lifq.multiple.output
 * @author LiFuqiang
 * @date 2016年5月19日 下午4:31:34
 * @version V1.0
 * @Description: 可以根据key值输出到多个文件
 * Update Logs:
 * **************************************************** * Name: * Date: * Description: ******************************************************
 */
package com.fastweb.cdnlog.bigdata.mapred.multiout;

import com.fastweb.cdnlog.bigdata.mapred.Constant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author LiFuqiang
 * @ClassName: MultipleOutputFormat
 * @date 2016年5月19日 下午4:31:34
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class MultiOutputFormat<K, V> extends FileOutputFormat<K, V> {
    private static final Log LOG = LogFactory.getLog(MultiOutputFormat.class);
    public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
    private RecordWriter<K, V> writer = null;

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        if (writer == null) {
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));
        }
        return writer;
    }


    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;
        OutputCommitter committer = super.getOutputCommitter(conf);
        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("Undefined job output-path");
            }
            workPath = outputPath;
        }
        //   System.out.println(workPath);
        return workPath;
    }

    /**
     * @param key
     * @param value
     * @param context
     * @return 产生的文件名不包含后缀名
     */
    protected String generateFileNameForKeyValue(K key, V value, TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        String str = conf.get(FileOutputFormat.OUTDIR) + Path.SEPARATOR + key.toString();
//        LOG.info(key.getClass());
//        LOG.info("output dir file name is :" + str);
        return str;
    }


    /**
     * Set whether the output of the job is compressed.
     *
     * @param job      the job to modify
     * @param compress should the output of the job be compressed?
     */
    public static void setCompressOutput(Job job, boolean compress) {
        job.getConfiguration().setBoolean(FileOutputFormat.COMPRESS, compress);
    }

    /**
     * Set the {@link CompressionCodec} to be used to compress job outputs.
     *
     * @param job        the job to modify
     * @param codecClass the {@link CompressionCodec} to be used to
     *                   compress the job outputs
     */
    public static void
    setOutputCompressorClass(Job job,
                             Class<? extends CompressionCodec> codecClass) {
        FileOutputFormat.setOutputCompressorClass(job, codecClass);
    }

    /**
     * Get the {@link CompressionCodec} for compressing the job outputs.
     *
     * @param job          the {@link Job} to look in
     * @param defaultValue the {@link CompressionCodec} to return if not set
     * @return the {@link CompressionCodec} to be used to compress the
     * job outputs
     * @throws IllegalArgumentException if the class was specified, but not found
     */
    public static Class<? extends CompressionCodec>
    getOutputCompressorClass(JobContext job,
                             Class<? extends CompressionCodec> defaultValue) {
        return FileOutputFormat.getOutputCompressorClass(job, defaultValue);
    }


    /**
     * Is the job output compressed?
     *
     * @param job the Job to look in
     * @return <code>true</code> if the job output should be compressed,
     * <code>false</code> otherwise
     */
    public static boolean getCompressOutput(JobContext job) {
        return FileOutputFormat.getCompressOutput(job);
        //return Boolean.valueOf(job.getConfiguration().get(Constant.MAPRED_OUTPUT_COMPRESS));
    }

    public class MultiRecordWriter extends RecordWriter<K, V> {
        /**
         * RecordWriter的缓存,K表示文件的完整路径
         */
        private HashMap<String, RecordWriter<K, V>> recordWriters = null;

        private TaskAttemptContext job = null;
        /**
         * 输出目录
         */
        private Path workPath = null;

        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
            super();
            this.job = job;
            this.workPath = workPath;
            recordWriters = new HashMap<String, RecordWriter<K, V>>();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<K, V>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            // 得到输出文件名
            String baseName = generateFileNameForKeyValue(key, value, job);
            RecordWriter<K, V> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                try {
                    rw = getBaseRecordWriter(job, baseName);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                this.recordWriters.put(baseName, rw);
            }
            rw.write(key, value);
        }

        // ${mapred.out.dir}/_temporary/_${taskid}/${nameWithExtension}
        private RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName)
                throws IOException, InterruptedException, ClassNotFoundException {
            Configuration conf = job.getConfiguration();
            boolean isCompressed = getCompressOutput(job);
            //boolean isCompressed = true;
            String keyValueSeparator = conf.get(SEPERATOR, "\t");
            CompressionCodec codec = null;
            String extension = "";
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        getOutputCompressorClass(job, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                extension = codec.getDefaultExtension();
            }

            Path file = new Path(baseName + extension);
            FileSystem fs = file.getFileSystem(conf);
            if (!isCompressed) {
                FSDataOutputStream fileOut = fs.create(file, false);
                return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
            } else {
                FSDataOutputStream fileOut = fs.create(file, false);
                return new LineRecordWriter<K, V>(new DataOutputStream
                        (codec.createOutputStream(fileOut)),
                        keyValueSeparator);
            }
        }
    }

    public static Path getDestinationPath(JobContext job) {
        return new Path(job.getConfiguration().get(Constant.ETL_DESTINATION_PATH));
    }

    public static void main(String[] args) {
        Path p = new Path("/user", "check");
        System.out.println(p.toString());
        Boolean bool = Boolean.valueOf("true");
        if (bool) {
            System.out.println(bool);
        }
    }


}
