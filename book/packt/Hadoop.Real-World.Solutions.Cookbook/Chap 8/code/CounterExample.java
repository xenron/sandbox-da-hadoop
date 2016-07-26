package com.packt.hadoop.solutions;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CounterExample extends Configured implements Tool{
	
	private static final String VALID_IP_ADDRESS = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
	        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	
	private static Pattern pattern;
    
	static enum BadRecords{INVALID_NUMBER_OF_COLUMNS,INVALID_IP_ADDRESS };
	
	

	public static class ValidateWeblogEntries extends Mapper<Object, Text, Text, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			
			if(pattern == null){
				pattern = Pattern.compile(VALID_IP_ADDRESS);;
			}
			
			String record = value.toString();
			String [] columns = record.split("\t");
			
			//Check for valid number of columns
			if (columns.length != 5){
				context.getCounter(BadRecords.INVALID_NUMBER_OF_COLUMNS).increment(1);
				return;
			}
			
			//Check for valid IP addresses
			Matcher matcher = pattern.matcher(columns[4]);
		    if(!matcher.matches()){
		    	context.getCounter(BadRecords.INVALID_IP_ADDRESS).increment(1);
				return;
		    }

			context.write( new Text(""), new Text(record) );
		}

	}

	public int run(String[] args) throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf, "CounterExample");
		job.setJarByClass(CounterExample.class);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(ValidateWeblogEntries.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		return 0;
	}

	public static void main(String[] args) throws Exception{
		
		int result = ToolRunner.run(new Configuration(), new CounterExample(), args);
		System.exit(result);
	}
}

