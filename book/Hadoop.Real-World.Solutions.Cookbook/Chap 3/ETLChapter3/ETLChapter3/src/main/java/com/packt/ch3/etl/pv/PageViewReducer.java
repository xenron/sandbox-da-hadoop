package com.packt.ch3.etl.pv;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageViewReducer extends Reducer<CompositeKey, Text, Text, LongWritable> {
    private LongWritable pageViews = new LongWritable();

    @Override
    protected void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String lastIp = null;
        long pages = 0;
        for(Text t : values) {
            String ip = t.toString();
            if (lastIp == null) {
                lastIp = ip;
                pages++;
            }
            else if (!lastIp.equals(ip)) {
                lastIp = ip;
                pages++;
            }
            else if (lastIp.compareTo(ip) > 0) {
                throw new IOException("secondary sort failed");
            }
        }
        pageViews.set(pages);
        context.write(key.getFirst(), pageViews);
    }
}
