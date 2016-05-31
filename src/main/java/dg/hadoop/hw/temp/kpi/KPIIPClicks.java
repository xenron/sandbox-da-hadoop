package dg.hadoop.hw.temp.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KPIIPClicks {
	/**
	 * IP提供的点击数：去重IP计数： 未去重IP总数：
	 * **/
	private static long totalIPs = 0;
	private static long InvalidLines = 0;
	public static class KPIIPClicksMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			KPI kpi;
			kpi = KPI.filterIPs(value.toString());
			if (kpi.isValid()) {
				word.set(kpi.getRemote_addr());
				output.collect(word, one);
				totalIPs++;
			}
			else
			{
				InvalidLines++;
				System.out.println("*Invalid Lines:"+InvalidLines+"*:"+value.toString());
			}

		}
	}

	public static class KPIIPClicksReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {
		private Text result = new Text();
		Set<String> count=new HashSet<String>();
		
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}
			count.add(key.toString());
			result.set("提供点击数:"+String.valueOf(sum)+" IP计数:"+count.size()+" 未去重IP总数:"+totalIPs);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hadoop0:9000/in-h5/access.20120104.log";
		String output = "hdfs://hadoop0:9000/out/kpiIPClicks";

		JobConf conf = new JobConf(KPIIPClicks.class);
		conf.setJobName("KPIIPClicks");
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(KPIIPClicksMapper.class);
		//conf.setCombinerClass(KPIIPClicksReducer.class);
		conf.setReducerClass(KPIIPClicksReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}
}
