package com.ch5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCounterExample {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "counters");
		table.incrementColumnValue(Bytes.toBytes("Jan14"), Bytes.toBytes("monthly"),
				Bytes.toBytes("hits"), 10L);
		
		table.close();
	}
}