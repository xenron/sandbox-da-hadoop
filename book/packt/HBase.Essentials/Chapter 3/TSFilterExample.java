package com.ch3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;


public class TSFilterExample {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "tab1");

		List<Long> ts = new ArrayList<Long>();
		ts.add(new Long(2));
		ts.add(new Long(10));
		//filter output the column values for specified timestamps
		Filter filter = new TimestampsFilter(ts);

		// Create Scan instance
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("Row Value: " + res);
		}
		scanner.close();
		table.close();
	}
}
