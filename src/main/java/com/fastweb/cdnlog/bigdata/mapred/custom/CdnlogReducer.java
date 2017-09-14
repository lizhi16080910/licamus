package com.fastweb.cdnlog.bigdata.mapred.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by lfq on 2016/9/7.
 */
public class CdnlogReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator iterator =  values.iterator();
        for(Text text :values){
            context.write(key,text);
        }
    }
}
