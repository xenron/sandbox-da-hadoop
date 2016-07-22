package com.ch5;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.security.AccessDeniedException;

// Sample access-control observer coprocessor. It utilizes RegionObserver
// and intercept preGet() method to check user privilege for the given table
// and column family.
public class ObserverCoprocessorEx extends BaseRegionObserver {
	// @Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> c,
			Get get, List<KeyValue> result) throws IOException {
		byte[] table = c.getEnvironment().getRegion().getRegionInfo()
				.getRegionName();

		// TODO Code for checking the permissions over table or region...

		if (ACCESS_NOT_ALLOWED) {
			throw new AccessDeniedException("User is not allowed for access");
		}
	}

	// Similarly override prePut(), preDelete(), etc. bease on the need..
}