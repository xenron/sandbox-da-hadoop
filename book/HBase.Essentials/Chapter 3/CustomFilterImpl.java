package com.ch3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements a custom filter for HBase. It takes a value and compares it with
 * every value in each cell checked. Once there is a match the entire row is
 * passed, otherwise filtered out.
 */

public class CustomFilterImpl extends FilterBase {

	private byte[] value = null;
	private boolean filterRow = true;

	public CustomFilterImpl() {
		super();
	}

	// Set the value to compare
	public CustomFilterImpl(byte[] value) {
		this.value = value;
	}

	// Reset filter flag for each row
	@Override
	public void reset() {
		this.filterRow = true;
	}

	@SuppressWarnings("deprecation")
	@Override
	public ReturnCode filterKeyValue(Cell cell) {
		// When there is a matching value, then let the row pass.
		if (Bytes.compareTo(value, cell.getValue()) == 0) {
			filterRow = false;
		}
		// Always use include.
		return ReturnCode.INCLUDE;
	}

	// Method for decision making based on the flag.
	@Override
	public boolean filterRow() {
		return filterRow;
	}

	// Writes the given value out so it can be send to the region servers.
	public void write(DataOutput dataOutput) throws IOException {
		Bytes.writeByteArray(dataOutput, this.value);
	}

	// Used by region servers to establish the filter instance with the correct
	// values.
	public void readFields(DataInput dataInput) throws IOException {
		this.value = Bytes.readByteArray(dataInput);
	}
}
