package com.packt.hadoop.hdfs.ch2.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SequenceWriter extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
        
        if (args.length < 2) {
            System.err.println("SequenceWriter [hdfs input path] [hdfs output dir]");
            return 1;
        }
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        Job weblogJob = new Job(conf);
        weblogJob.setJobName("Sequence File Writer");
        weblogJob.setJarByClass(getClass());
        weblogJob.setNumReduceTasks(0);
        weblogJob.setMapperClass(IdentityMapper.class);        
        weblogJob.setMapOutputKeyClass(LongWritable.class);
        weblogJob.setMapOutputValueClass(Text.class);
        weblogJob.setOutputKeyClass(LongWritable.class);
        weblogJob.setOutputValueClass(Text.class);
        weblogJob.setInputFormatClass(TextInputFormat.class);
        weblogJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        SequenceFileOutputFormat.setOutputPath(weblogJob, outputPath);
       
        if(weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new SequenceWriter(), args);
        System.exit(returnCode);
    }
}
