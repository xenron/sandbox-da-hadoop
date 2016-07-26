package com.ch3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class RowFilterExample {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "tab1");

		// Create Scan instance
		Scan scan = new Scan();

		// Add a column with value "Hello", in “cf1:greet”, to the Put.
		scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("greet"));

		// Filter using the regular expression
		Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator("*-o"));
		
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
