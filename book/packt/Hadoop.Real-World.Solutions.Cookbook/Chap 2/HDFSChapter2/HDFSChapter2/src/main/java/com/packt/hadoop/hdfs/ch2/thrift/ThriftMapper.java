package com.packt.hadoop.hdfs.ch2.thrift;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ThriftMapper extends Mapper<Object, Text, NullWritable, ThriftWritable<WeblogRecord>> {

    private ThriftWritable<WeblogRecord> thriftRecord = ThriftWritable.newInstance(WeblogRecord.class);
    private WeblogRecord record = new WeblogRecord();
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
        record.setCookie(cookie);
        record.setPage(page);
        record.setTimestamp(timestamp.getTime());
        record.setIp(ip);
        thriftRecord.set(record);
        context.write(NullWritable.get(), thriftRecord);
    }
}
