package com.ch4;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HBaseMRTest {

	// Mapper Class
	static class HBaseTestMapper extends TableMapper<Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		@SuppressWarnings("deprecation")
		@Override
		public void map(ImmutableBytesWritable rowKey, Result columns,
				Context context) throws IOException, InterruptedException {
			try {

				HashMap<String, String> customerMap = new HashMap<String, String>();
				for (KeyValue kv : columns.raw()) {
					String qualifier = "";
					HashMap<?, ?> kvMap = (HashMap<?, ?>) kv.toStringMap();
					String colFamily = (String) kvMap.get("family");
					if (colFamily.equalsIgnoreCase("cf1")) {
						if (kvMap.get("qualifier") != null
								&& !kvMap.get("qualifier").equals("")) {
							qualifier = (String) kvMap.get("qualifier");
						}
						String qualifierVal = Bytes
								.toString(kv.getValueArray());
						customerMap.put(qualifier, qualifierVal);
					}
				}
				System.out.println(customerMap.toString());

				text.set(rowKey.toString());
				context.write(text, ONE);

			} catch (RuntimeException e) {
				e.printStackTrace();
			}
		}
	}

	// Reducer Class
	static class HBaseTestReducer extends
	TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		protected void reduce(Text rowKey, Iterable<IntWritable> columns,
				Context context) throws IOException, InterruptedException {
			try {
				for (IntWritable values : columns) {
					System.out.println(values);
				}
				Put put = new Put(Bytes.toBytes(rowKey.toString()));
				put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col-1"),
						Bytes.toBytes(rowKey.toString()));

				context.write(null, put);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Main Driver Class
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		try {

			// Setup Configuraiton
			Configuration config = new Configuration();
			config.clear();
			config.set("hbase.zookeeper.quorum", "localhost");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.set("hbase.master", "localhost:60000");

			Job job = new Job(config, "Read Write from Customer Table");
			job.setJarByClass(HBaseMRTest.class);

			Scan scan = new Scan();
			// 1 is the default in Scan
			scan.setCaching(1000);
			scan.setCacheBlocks(false);

			// define input hbase table
			TableMapReduceUtil.initTableMapperJob("tab1", // input table
					scan, // Scan instance to control CF and attribute selection
					HBaseTestMapper.class, // mapper class
					Text.class, // mapper output key
					IntWritable.class, // mapper output value
					job);

			// define output table
			TableMapReduceUtil.initTableReducerJob(
					"tab1Copy", // output table
					HBaseTestReducer.class, // reducer class
					job);

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			System.out.println("MR Execution Error");
			System.exit(1);
		}
	}
}