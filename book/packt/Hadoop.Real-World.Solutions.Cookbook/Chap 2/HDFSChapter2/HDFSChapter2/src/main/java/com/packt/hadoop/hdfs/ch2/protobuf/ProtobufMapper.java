package com.packt.hadoop.hdfs.ch2.protobuf;


import com.twitter.elephantbird.mapreduce.io.ProtobufWritable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ProtobufMapper extends Mapper<Object, Text, NullWritable, ProtobufWritable<WeblogRecord.Record>> {

    private ProtobufWritable<WeblogRecord.Record> protobufRecord = ProtobufWritable.newInstance(WeblogRecord.Record.class);
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String cookie = tokens[0];
        String page = tokens[1];
        String date = tokens[2];
        String time = tokens[3];
        String formatedDate = date + ":" + time;
        Date timestamp = null;
        try {
            timestamp = dateFormatter.parse(formatedDate);
        } catch(ParseException ex) {
            return;
        }
        String ip = tokens[4];
        protobufRecord.set(WeblogRecord.Record.newBuilder()
                .setCookie(cookie)
                .setPage(page)
                .setTimestamp(timestamp.getTime())
                .setIp(ip)
                .build());
        context.write(NullWritable.get(), protobufRecord);
    }
}
