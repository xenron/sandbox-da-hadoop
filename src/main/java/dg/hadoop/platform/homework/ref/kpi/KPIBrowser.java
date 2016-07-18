package dg.hadoop.platform.homework.ref.kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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


public class KPIBrowser {
/**
 * 统计用户使用的浏览器种类，计算出各种浏览器占的百分比
 * **/
	private static long total=0;
	
	public static class KPIBrowserMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
        private Text browserInfo = new Text();
        private IntWritable one=new IntWritable(1);
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            
        	KPI kpi = KPI.filterBroswer(value.toString());
            if (kpi.isValid()) {
					browserInfo.set(kpi.getHttp_user_agent());
					total++;
					output.collect(browserInfo, one);
            } 
        }
    }

    public static class KPIBrowserReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
        private Text result= new Text();
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	int sum=0;
        	while (values.hasNext()) {
            		sum += values.next().get();
					//sum+=Integer.parseInt(values.next().toString());	
            }
        	//float percent = (float) sum / (float) ipnum * 100;
            result.set(String.valueOf(((double)sum/total*100)+"%"));
            output.collect(key,result);
		}
    }

    public static void main(String[] args) throws Exception {
    	String input = "hdfs://hadoop0:9000/in-h5/access.20120104.log";
		String output = "hdfs://hadoop0:9000/out/kpiBrowser";

        JobConf conf = new JobConf(KPIBrowser.class);
        conf.setJobName("KPIBrowser");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(KPIBrowserMapper.class);
		//conf.setCombinerClass(KPIBrowserReducer.class);
		conf.setReducerClass(KPIBrowserReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		
		System.exit(0);
    }
}
