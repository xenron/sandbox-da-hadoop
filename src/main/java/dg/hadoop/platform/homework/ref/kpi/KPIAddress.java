package dg.hadoop.platform.homework.ref.kpi;

//import java.io.FileNotFoundException;

/*import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;*/
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import dg.hadoop.platform.homework.ref.iplocation.IPSeeker;
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
//import org.apache.hadoop.mapreduce.Mapper.Context;


public class KPIAddress {
	/**
	 *统计用户来自的地域（各省、直辖市、自治区，国外），计算各地域访问占的百分比
	 * ***/

	public static int ipnum = 0;

	public static class KPIAddressMapper extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text ips = new Text();

		//@Override
		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			KPI kpi = KPI.filterIPs(value.toString());
			if (kpi.isValid()) {
				ips.set(kpi.getRemote_addr());
				output.collect(ips, one);
			}
		}
	}

	public static class KPIAddressReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {

		IPSeeker ipseeker = IPSeeker.getInstance();

		//@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// ip to address
			String address = ipseeker.getAddress(key.toString());
			int sum = 0;
			while (values.hasNext()) {
				sum = sum + values.next().get();
			}
			String outvalue = Integer.toString(sum) + "+" + address;
			output.collect(key, new Text(outvalue));
		}
	}

	public static class KPIAddressMapper2 extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {
		//@Override
		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String str = "";

			while (tokenizer.hasMoreTokens()) {
				String tmp = tokenizer.nextToken();

				if (tmp.contains("+")) {
					str = tmp;
					break;
				}
			}
			int sep = str.indexOf("+");
			int num = Integer.parseInt(str.substring(0, sep));
			String address = str.substring(sep + 1);
			ipnum = ipnum + num;
			// System.out.println(address + "\t" + Integer.toString(num));
			output.collect(new Text(address), new IntWritable(num));

		}
	}
	public static class KPIAddressReducer2 extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {
		IPSeeker ipseeker = IPSeeker.getInstance();

		//@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum = sum + values.next().get();
			}
			float percent = (float) sum / (float) ipnum * 100;
			String tmp = "	" + String.valueOf(percent) + "%";
			output.collect(key, new Text(tmp));
		}
	}

	public static void main(String[] args) throws Exception {
		String input = "hdfs://hadoop0:9000/in-h5/access.20120104.log";
		String output = "hdfs://hadoop0:9000/out/ipAddress";

		Path tempDir = new Path("hdfs://hadoop0:9000/temp");

		JobConf conf = new JobConf(KPIAddress.class);
		conf.setJobName("KPIAddress");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(KPIAddressMapper.class);
		// conf.setCombinerClass(null);
		conf.setReducerClass(KPIAddressReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, (tempDir));

		JobClient.runJob(conf);

		JobConf conf2 = new JobConf(KPIAddress.class);
		conf2.setJobName("KPIAddress2");

		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(IntWritable.class);

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(KPIAddressMapper2.class);
		// conf.setCombinerClass(null);
		conf2.setReducerClass(KPIAddressReducer2.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, tempDir);
		FileOutputFormat.setOutputPath(conf2, new Path(output));

		JobClient.runJob(conf2);
		System.exit(0);
	}

}
