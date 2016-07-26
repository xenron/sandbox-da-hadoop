package com.packt.ch3.etl.geo;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class GeoRecordReader extends RecordReader<GeoKey, GeoValue> {

    private GeoKey key;
    private GeoValue value;
    private LineRecordReader reader = new LineRecordReader();
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        reader.initialize(is, tac);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
       
        boolean gotNextKeyValue = reader.nextKeyValue();
        if(gotNextKeyValue) {
            if (key == null) {
                key = new GeoKey();
            }
            if (value == null) {
                value = new GeoValue();
            }
            Text line = reader.getCurrentValue();
            String[] tokens = line.toString().split("\t");
            key.setLocation(new Text(tokens[0]));
            key.setLatitude(new FloatWritable(Float.parseFloat(tokens[4])));
            key.setLongitude(new FloatWritable(Float.parseFloat(tokens[5])));
            
            value.setActor(new Text(tokens[3]));
            value.setEventDate(new Text(tokens[1]));
            value.setEventType(new Text(tokens[2]));
            try {
                value.setFatalities(new IntWritable(Integer.parseInt(tokens[7])));
            } catch(NumberFormatException ex) {
                value.setFatalities(new IntWritable(0));
            }
            value.setSource(new Text(tokens[6]));
        }
        else {
            key = null;
            value = null;
        }
        return gotNextKeyValue;
    }

    @Override
    public GeoKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public GeoValue getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
    
}
