package com.packt.hadoop.hdfs.ch2.protobuf;

import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProtobufWriter extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        Job weblogJob = new Job(conf);
        weblogJob.setJobName("ProtobufWriter");
        weblogJob.setJarByClass(getClass());
        weblogJob.setNumReduceTasks(0);
        weblogJob.setMapperClass(ProtobufMapper.class);        
        weblogJob.setMapOutputKeyClass(LongWritable.class);
        weblogJob.setMapOutputValueClass(Text.class);
        weblogJob.setOutputKeyClass(LongWritable.class);
        weblogJob.setOutputValueClass(Text.class);
        weblogJob.setInputFormatClass(TextInputFormat.class);
        weblogJob.setOutputFormatClass(LzoProtobufBlockOutputFormat.class);
        
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        LzoProtobufBlockOutputFormat.setClassConf(WeblogRecord.Record.class, weblogJob.getConfiguration());
        LzoProtobufBlockOutputFormat.setOutputPath(weblogJob, outputPath);
        
       
        if(weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new ProtobufWriter(), args);
        System.exit(returnCode);
    }
}
