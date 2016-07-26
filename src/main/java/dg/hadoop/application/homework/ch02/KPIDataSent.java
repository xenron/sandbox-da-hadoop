package dg.hadoop.application.homework.ch02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


public class KPIDataSent {
	/***
	 * 计算该天的被传输页面的总字节数
	 * **/
	public static class KPIDataSentMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, DoubleWritable> {
		private DoubleWritable pageSize=new DoubleWritable();
		private Text word = new Text();
		
		
		
		public void map(Object key, Text value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			KPI kpi;

			kpi = KPI.filterPVs(value.toString());
			if (kpi.isValid()) {
				
				try {
					word.set(kpi.getRequest());
					pageSize.set(Double.valueOf(kpi.getBody_bytes_sent()));
					output.collect(word, pageSize);
				} catch (NumberFormatException e) {
					System.out.println(kpi.getRequest()+"\t"+kpi.getBody_bytes_sent());
				}
			}
		}
	}

	public static class KPIDataSentReducer extends MapReduceBase implements
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			Double sum = 0.0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://xenron-XPS-8700:9000/input/access.20120104.log";
		String output = "hdfs://xenron-XPS-8700:9000/output/hw3";

		JobConf conf = new JobConf(KPIDataSent.class);
		conf.setJobName("KPIDataSent");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(DoubleWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(KPIDataSentMapper.class);
		conf.setCombinerClass(KPIDataSentReducer.class);
		conf.setReducerClass(KPIDataSentReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}
}
