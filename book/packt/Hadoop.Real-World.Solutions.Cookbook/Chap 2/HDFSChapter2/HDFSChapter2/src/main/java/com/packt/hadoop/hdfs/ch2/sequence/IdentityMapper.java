package com.packt.hadoop.hdfs.ch2.sequence;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IdentityMapper extends Mapper<Object, Object, Object, Object> {
    
    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
    
}
