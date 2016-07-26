package dg.hadoop.application.homework.ch02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class KPIPV {
	/*
	 * PV(PageView): 页面访问量统计
	 * */

	public static class KPIPVMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			KPI kpi;
			
				kpi = KPI.filterPVs(value.toString());
				if (kpi.isValid()) {
					word.set(kpi.getRequest());
					output.collect(word, one);
				}

		}
	}

	public static class KPIPVReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://xenron-XPS-8700:9000/input/access.20120104.log";
		String output = "hdfs://xenron-XPS-8700:9000/output/hw2";

		JobConf conf = new JobConf(KPIPV.class);
		conf.setJobName("KPIPV");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(KPIPVMapper.class);
		//conf.setCombinerClass(KPIPVReducer.class);
		conf.setReducerClass(KPIPVReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}