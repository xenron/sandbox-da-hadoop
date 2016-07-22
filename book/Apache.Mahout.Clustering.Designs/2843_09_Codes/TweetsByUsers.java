package com.packt.test;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TweetsByUsers extends Configured implements Tool {
	public static class UserAsKeyMapper extends Mapper<LongWritable,Text,Text,Text>{
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
				String[] fields = value.toString().split("\t");
				if (fields.length - 1 < 1 ||fields.length - 1 < 0) {
					context.getCounter(	"Map", "LinesWithErrors").increment(1);
					return;
				}
				String userName = fields[0];
				String tweet = fields[1];
				context.write(new Text(userName), new Text(tweet));
				}
		
	}
	
	public static class UserAsKeyReducer extends Reducer<Text, Text, Text, BytesWritable>{
		protected void reduce(Text key,	Iterable<Text> values,Context context) throws IOException,InterruptedException {			
				StringBuilder output = new StringBuilder();
				for (Text value : values) {
					output.append(value.toString()).append(" ");
				}
				context.write(key, new BytesWritable(output.toString().trim().getBytes()));
		}
		
	}
	
	public int run(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: input and output <in> <out>");
		      System.exit(2);
		 }
	    Job job = Job.getInstance(conf,"TweetsByUser");
	    job.setJarByClass(TweetsByUsers.class);
	    job.setMapperClass(UserAsKeyMapper.class);
	    job.setReducerClass(UserAsKeyReducer.class);    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class); 
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(ByteWritable.class);
	    Path outputPath = new Path(otherArgs[1]);
	    FileInputFormat.addInputPath(job,new Path(otherArgs[0]));			
	    FileOutputFormat.setOutputPath(job, outputPath);
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    try {
			return(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException | InterruptedException e) {
			
			e.printStackTrace();
			return 0;
		}
	    
	}

	public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new TweetsByUsers(), args);
	  System.exit(exitCode);
	}


}
