package com.packt.ch3.etl.pv;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageViewMapper extends Mapper<Object, Text, CompositeKey, Text> {
    private CompositeKey compositeKey = new CompositeKey();
    private Text first = new Text();
    private Text second = new Text();
    private Text outputValue = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        if (tokens.length > 3) {
            String page = tokens[2];
            String ip = tokens[0];
            first.set(page);
            second.set(ip);
            compositeKey.setFirst(first);
            compositeKey.setSecond(second);
            outputValue.set(ip);
            context.write(compositeKey, outputValue);
        }
    }
}
