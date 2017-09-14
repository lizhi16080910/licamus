package com.fastweb.cdnlog.bigdata.merge;

import com.fastweb.cdnlog.bigdata.outputformat.multiout.LineRecordWriter;
import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lfq on 2016/11/28.
 */
public class MultipleOut {
    //String: key
    private Map<String, RecordWriter> out = new HashMap<>();

   public RecordWriter getWriter(String key,Reducer.Context context) throws IOException {
       RecordWriter writer = out.get(key);
       Path file = new Path("");
       if(writer == null){
           FileSystem fs = file.getFileSystem(context.getConfiguration());
           Class<? extends CompressionCodec> codecClass = LzopCodec.class;
           CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, context.getConfiguration());

           FSDataOutputStream fileOut = fs.create(file, false);
           writer = new LineRecordWriter<String, Text>(new DataOutputStream
                   (codec.createOutputStream(fileOut)),
                   "");
       }
       return writer;
   }

}
