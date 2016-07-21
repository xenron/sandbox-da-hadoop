package com.packt.ch3.etl.geo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoFilter extends Configured implements Tool {
    
    public static class GeoFilterMapper extends Mapper<GeoKey, GeoValue, Text, IntWritable> {
        @Override
        protected void map(GeoKey key, GeoValue value, Context context) throws IOException, InterruptedException {
            String location = key.getLocation().toString();
            if (location.toLowerCase().equals("aba")) {
                context.write(value.getActor(), value.getFatalities());
            }
        }
    }
    
    public int run(String[] args) throws Exception {
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        Job geoJob = new Job(conf);
        geoJob.setNumReduceTasks(0);
        geoJob.setJobName("GeoFilter");
        geoJob.setJarByClass(getClass());
        geoJob.setMapperClass(GeoFilterMapper.class);        
        geoJob.setMapOutputKeyClass(Text.class);
        geoJob.setMapOutputValueClass(IntWritable.class);
        //geoJob.setOutputKeyClass(Text.class);
        //geoJob.setOutputValueClass(Text.class);
        geoJob.setInputFormatClass(GeoInputFormat.class);
        geoJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(geoJob, inputPath);
        FileOutputFormat.setOutputPath(geoJob, outputPath);
       
        if(geoJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new GeoFilter(), args);
        System.exit(returnCode);
    }
}
