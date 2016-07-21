package com.packt.ch5.advjoin.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeblogMapper extends Mapper<Object, Text, Text, Text> {

    public static final String IP_COUNTRY_TABLE_FILENAME = "nobots_ip_country_tsv.txt";
    private Map<String, String> ipCountryMap = new HashMap<String, String>();
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for(Path p : files) {
            if (p.getName().equals(IP_COUNTRY_TABLE_FILENAME)) {
                BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
                String line = reader.readLine();
                while(line != null) {
                    String[] tokens = line.split("\t");
                    String ip = tokens[0];
                    String country = tokens[1];
                    ipCountryMap.put(ip, country);
                    line = reader.readLine();
                }
            }
        }
        
        if (ipCountryMap.isEmpty()) {
            throw new IOException("unable to load ip country table");
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] tokens = row.split("\t");
        String ip = tokens[0];
        String country = ipCountryMap.get(ip);
        outputKey.set(country);
        outputValue.set(row);
        context.write(outputKey, outputValue);
    }
    
}
