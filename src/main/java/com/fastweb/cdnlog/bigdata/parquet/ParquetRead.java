package com.fastweb.cdnlog.bigdata.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * Created by lfq on 2016/12/23.
 */
public class ParquetRead {
    public static void main(String[] args) throws IOException {
        //Footer, 存储了文件的元数据信息和统计信息
    }

    public static void read() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.100.14:8020");
        GroupReadSupport readSupport = new GroupReadSupport();
        Path path= new Path("/user/hive/warehouse/cdnlog_error_status/usercode_=1/month_=201605/day_=01/d74d25c26c26444f-284026fae85c9ec2_267931344_data.0.parq");
        ParquetReader<Group> reader = new ParquetReader<Group>(conf,path,readSupport);

        Group result = null;
        int count = 0;

        while((result = reader.read()) != null) {
            // System.out.println(result.getType());
            SimpleGroup simpleGroup = (SimpleGroup)result;
            System.out.println(simpleGroup.getString("machine",0));
            count ++ ;
            if(count == 5){
                break;
            }
        }
        reader.close();
    }

    public static void write() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary left (UTF8);\n" +
                " required binary right (UTF8);\n" +
                "}");

        GroupFactory factory = new SimpleGroupFactory(schema);

        Group group = factory.newGroup().append("left","L").append("right","R");

        Path path = new Path("data.parquet");

        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        writeSupport.setSchema(schema,configuration);

        ParquetWriter<Group> writer = new ParquetWriter<Group>(path,writeSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0,
                configuration
        );

        writer.write(group);
        writer.close();
    }

}
