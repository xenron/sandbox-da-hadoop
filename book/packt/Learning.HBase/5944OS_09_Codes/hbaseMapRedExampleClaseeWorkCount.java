import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;


public class hbaseMapRedExampleClaseeWorkCount {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable count = new IntWritable(1);
		private Text textToEmit = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer strTokenizerObj = new StringTokenizer(value.toString());
			while (strTokenizerObj.hasMoreTokens()) {
				textToEmit.set(strTokenizerObj.nextToken());
				context.write(textToEmit, count);
			}
		}
	}
	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				total += iterator.next().get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("colFam"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(total)));
			context.write(NullWritable.get(), put);
		}
	}
	public static void createHBaseTable(String hbaseMapRedTestTableObj) throws IOException {
		HTableDescriptor tableDescriptorObj = new HTableDescriptor(hbaseMapRedTestTableObj);
		HColumnDescriptor column = new HColumnDescriptor("colFam");
		tableDescriptorObj.addFamily(column);
		Configuration configObj = HBaseConfiguration.create();
		configObj.set("hbase.zookeeper.quorum", "infinity");
		configObj.set("hbase.zookeeper.property.clientPort", "2222");
		HBaseAdmin hAdmin = new HBaseAdmin(configObj);
		if (hAdmin.tableExists(hbaseMapRedTestTableObj)) {
			System.out.println("Table exist !");
			hAdmin.disableTable(hbaseMapRedTestTableObj);
			hAdmin.deleteTable(hbaseMapRedTestTableObj);
		}
		System.out.println("Create Table" + hbaseMapRedTestTableObj);
		hAdmin.createTable(tableDescriptorObj);
	}
	public static void main(String[] args) throws Exception {
		String hbaseMapRedTestTableObj = "hbaseMapReduceTest";
		hbaseMapRedExampleClaseeWorkCount.createHBaseTable(hbaseMapRedTestTableObj);
		Configuration configObj = new Configuration();
		configObj.set("mapred.job.tracker", "infinity:9001");
		configObj.set("hbase.zookeeper.quorum", "infinity");
		configObj.set("hbase.zookeeper.property.clientPort", "2222");
		configObj.set(TableOutputFormat.OUTPUT_TABLE, hbaseMapRedTestTableObj);
		Job job = new Job(configObj, "HBase WordCount Map reduce");
		job.setJarByClass(hbaseMapRedExampleClaseeWorkCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("<hbasefilepath>"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}