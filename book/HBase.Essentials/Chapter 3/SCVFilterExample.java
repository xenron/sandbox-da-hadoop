package com.ch3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class SCVFilterExample {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "tab1");

		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("cf1"), Bytes.toBytes("greet"),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator("Hello"));

		// By Default it is false.
		// If set as true, this restricts the rows
		// if the specified column is not present
		filter.setFilterIfMissing(true);

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
