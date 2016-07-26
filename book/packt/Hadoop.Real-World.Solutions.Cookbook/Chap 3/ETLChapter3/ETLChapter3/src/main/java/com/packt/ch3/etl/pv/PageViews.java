package com.packt.ch3.etl.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageViews extends Configured implements Tool {
    
    static class CompositeKeyParitioner extends Partitioner<CompositeKey, Writable> {
        
        @Override
        public int getPartition(CompositeKey key, Writable value, int numParition) {
            return (key.getFirst().hashCode() &  0x7FFFFFFF) % numParition;
        }
    }
    
    static class GroupComparator extends WritableComparator {
        public GroupComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey lhs = (CompositeKey)a;
            CompositeKey rhs = (CompositeKey)b;
            return lhs.getFirst().compareTo(rhs.getFirst());
        }
    }
    
    static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey lhs = (CompositeKey)a;
            CompositeKey rhs = (CompositeKey)b;
            int cmp = lhs.getFirst().compareTo(rhs.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return lhs.getSecond().compareTo(rhs.getSecond());
        }
    }
    
    public int run(String[] args) throws Exception {
        
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        Configuration conf = getConf();
        Job weblogJob = new Job(conf);
        weblogJob.setJobName("PageViews");
        weblogJob.setJarByClass(getClass());
        weblogJob.setMapperClass(PageViewMapper.class);        
        weblogJob.setMapOutputKeyClass(CompositeKey.class);
        weblogJob.setMapOutputValueClass(Text.class);
        
        weblogJob.setPartitionerClass(CompositeKeyParitioner.class);
        weblogJob.setGroupingComparatorClass(GroupComparator.class);
        weblogJob.setSortComparatorClass(SortComparator.class);
        
        weblogJob.setReducerClass(PageViewReducer.class);
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
        int returnCode = ToolRunner.run(new PageViews(), args);
        System.exit(returnCode);
    }
}
