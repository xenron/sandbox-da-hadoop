package dg.hadoop.platform.homework.ref.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
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

public class KPIIP {
	/**
	 * 根据上述日志文件，计算该天的独立ip数
	 * **/
	private static long totalIPs=0;

	public static class KPIIPMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text ips = new Text();
        
        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
        	KPI kpi = KPI.filterIPs(value.toString());
            if (kpi.isValid()) {
                word.set(kpi.getRequest());
            	//word.set(kpi.getRequest());//http://www.itpub.net/thread-1417576-1-1.html	该页面独立IP数:1 总独立IP数:47196 总IP数:2910085
                ips.set(kpi.getRemote_addr());
                totalIPs++;
                
                output.collect(word, ips);
            }    
        }
    }
    public static class KPIIPReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> countTotal = new HashSet<String>();
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	Set<String> count = new HashSet<String>();//for each page
        	String str="";
        	while (values.hasNext()) {
        		str=values.next().toString();
                count.add(str);
                countTotal.add(str);               
            }
        	result.set(String.valueOf("该页面独立IP数:"+count.size()+" 总独立IP数:"+countTotal.size()+" 总IP数:"+totalIPs));
            output.collect(key, result);
        	
        }
    }

    public static void main(String[] args) throws Exception {
    	String input = "hdfs://hadoop0:9000/in-h5/access.20120104.log";
		String output = "hdfs://hadoop0:9000/out/kpiIP2";

        JobConf conf = new JobConf(KPIIP.class);
        conf.setJobName("KPIIP");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(KPIIPMapper.class);
        //conf.setCombinerClass(KPIIPReducer.class);
        conf.setReducerClass(KPIIPReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }

}