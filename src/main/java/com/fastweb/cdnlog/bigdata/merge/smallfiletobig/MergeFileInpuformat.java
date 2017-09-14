package com.fastweb.cdnlog.bigdata.merge.smallfiletobig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by lfq on 2017/3/15.
 */
public class MergeFileInpuformat extends InputFormat<NullWritable, Text> {
    public static final String MERGE_INPUT_PATH = "merge.input.path";

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String dir = conf.get(MERGE_INPUT_PATH);

        FileSystem fs = FileSystem.get(conf);
        List<InputSplit> listInputSplit = new ArrayList<>();
        Path path = new Path(dir);

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, true);

        Map<String, Integer> map = new HashMap<>();
        while (iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            String pathStr = locatedFileStatus.getPath().toString();
            String domain = pathStr.split("/")[7];
            Integer a = map.get(domain);
            if (a == null) {
                map.put(domain, 1);
                continue;
            }
            map.put(domain, a + 1);
        }
        System.out.println(map.size());

        StringBuilder domains = null;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > 1) {
                listInputSplit.add(new MergeFileInputSplit(entry.getKey()));
            } else {
                if(domains == null){
                    domains = new StringBuilder();
                }
                domains.append(entry.getKey() + ",");
            }
        }
        if(domains != null){
            listInputSplit.add(new MergeFileInputSplit(domains.toString()));
        }
        return listInputSplit;
    }



    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MergeFileRecorder();
    }

    public static void setInputPath(Job job, Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        path = path.getFileSystem(conf).makeQualified(path);
        conf.set(MERGE_INPUT_PATH, StringUtils.escapeString(path.toString()));
    }

    public static class MergeFileInputSplit extends InputSplit implements Writable {

        // 既可以表示多个域名也可以表示单个域名，多个域名之间以，分割
        private String domains = null;
        private int length = 0;

        public MergeFileInputSplit() {
        }

        public MergeFileInputSplit(String domains) {
            this.domains = domains;
            this.length = domains.getBytes().length;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.length);
            out.write(this.domains.getBytes());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.length = in.readInt();
            byte[] bytes = new byte[this.length];
            in.readFully(bytes);
            this.domains = new String(bytes);
            System.out.println(this.domains);
        }
    }

    public static class MergeFileRecorder extends RecordReader<NullWritable, Text> {

        private String domains = null;
        private boolean next = true;

        public MergeFileRecorder() {

        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            MergeFileInputSplit mfSplit = (MergeFileInputSplit) split;
            this.domains = mfSplit.domains;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            System.out.println("MergeFileRecorder next key value" + next);
            if (next) {
                next = false;
                return true;
            }

            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return new Text(this.domains);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }

}
