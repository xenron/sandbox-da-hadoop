import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StatusMessageTest extends Configured implements Tool{

	
	public static class StatusMap extends Mapper<LongWritable, Text, LongWritable, Text>{
		
		private int rowCount = 0;
		private long startTime = 0;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			//Display rows per second every 100,000 rows
			rowCount++;
			if(startTime == 0 || rowCount % 100000 == 0)
			{
				if(startTime > 0)
				{
					long estimatedTime = System.nanoTime() - startTime;
					context.setStatus("Processing: " + (double)rowCount / ((double)estimatedTime/1000000000.0) + " rows/second");
					rowCount = 0;
				}
				
				startTime = System.nanoTime();
			}
			
			context.write(key, value);
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Job job = new Job(getConf());
		job.setJarByClass(StatusMessageTest.class);
		job.setJobName("StatusMessageTest");
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(StatusMap.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		
		return success ? 0 : 1;
		
	}
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new StatusMessageTest(), args);
		System.exit(ret);
	}

}
