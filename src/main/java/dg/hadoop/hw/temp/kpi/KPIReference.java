package dg.hadoop.hw.temp.kpi;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
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

public class KPIReference {
	/**
	 * 2 统计来源网站，列出域名及带来的独立ip数
	 * ***/
	public static class KPIReferenceMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text httpRefer = new Text();
        private Text remoteIps = new Text();

        public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	KPI kpi = KPI.filterDomain(value.toString());
            if (kpi.isValid()) {
            	httpRefer.set(kpi.getHttp_referer_domain());
                remoteIps.set(kpi.getRemote_addr());
                output.collect(httpRefer, remoteIps);
            }
            //System.out.println(kpi.getHttp_referer()+"***"+kpi.isValid());
        }
    }

    public static class KPIReferenceReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private Set<String> totalIP=new HashSet<String>();
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	Set<String> count = new HashSet<String>();
        	while (values.hasNext()) {
        		String str=values.next().toString();
                count.add(str);
                totalIP.add(str);
            }
            result.set("带来的独立IP:"+String.valueOf(count.size()));
            //result.set("带来的独立Ip数:"+String.valueOf(count.size()+"\t总IP数："+totalIP.size()));
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	String input = "hdfs://hadoop0:9000/in-h5/access.20120104.log";
		String output = "hdfs://hadoop0:9000/out/kpiReference";

        JobConf conf = new JobConf(KPIReference.class);
        conf.setJobName("KPIReference");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(KPIReferenceMapper.class);
        //conf.setCombinerClass(KPIReferenceReducer.class);
        conf.setReducerClass(KPIReferenceReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }
}
