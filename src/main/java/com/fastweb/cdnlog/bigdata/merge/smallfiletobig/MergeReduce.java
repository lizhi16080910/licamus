package com.fastweb.cdnlog.bigdata.merge.smallfiletobig;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lfq on 2017/3/15.
 */
public class MergeReduce extends Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            context.write(NullWritable.get(),value);
        }
    }
}
