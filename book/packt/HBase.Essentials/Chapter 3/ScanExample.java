package com.ch3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanExample {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "tab1");

		// Create Scan instance
		Scan scan = new Scan();

		// Add a column with value "Hello", in “cf1:greet”, to the Put.
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"));

		// Set Start Row
		scan.setStartRow(Bytes.toBytes("row-5"));

		// Set End Row
		scan.setStopRow(Bytes.toBytes("row-10"));

		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("Row Value: " + res);
		}
		scanner.close();
		table.close();
	}
}
