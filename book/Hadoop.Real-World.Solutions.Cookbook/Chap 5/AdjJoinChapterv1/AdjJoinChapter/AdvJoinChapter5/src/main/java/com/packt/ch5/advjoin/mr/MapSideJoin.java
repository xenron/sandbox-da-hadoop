package com.packt.ch5.advjoin.mr;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoin extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        DistributedCache.addCacheFile(new URI("/user/jowens/nobots_ip_country_tsv.txt"), conf);
        Job weblogJob = new Job(conf);
        weblogJob.setJobName("MapSideJoin");
        weblogJob.setNumReduceTasks(0);
        weblogJob.setJarByClass(getClass());
        weblogJob.setMapperClass(WeblogMapper.class);        
        weblogJob.setMapOutputKeyClass(Text.class);
        weblogJob.setMapOutputValueClass(Text.class);
        weblogJob.setOutputKeyClass(Text.class);
        weblogJob.setOutputValueClass(Text.class);
        weblogJob.setInputFormatClass(TextInputFormat.class);
        weblogJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(weblogJob, inputPath);
        FileOutputFormat.setOutputPath(weblogJob, outputPath);
        
       
        if(weblogJob.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
    
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new MapSideJoin(), args);
        System.exit(returnCode);
    }
}
