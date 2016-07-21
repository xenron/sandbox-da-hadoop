package com.packt.ch3.etl.pv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CompositeKey implements WritableComparable {

    private Text first = null;
    private Text second = null;
    
    public CompositeKey() {
        
    }
    public CompositeKey(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }
    
    public void write(DataOutput d) throws IOException {
        first.write(d);
        second.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (first == null) {
            first = new Text();
        }
        if (second == null) {
            second = new Text();
        }
        first.readFields(di);
        second.readFields(di);
    }

    public int compareTo(Object o) {
        CompositeKey other = (CompositeKey) o;
        int cmp = first.compareTo(other.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(other.getSecond());
    }

    @Override
    public boolean equals(Object obj) {
        CompositeKey other = (CompositeKey)obj;
        return first.equals(other.getFirst());
    }

    @Override
    public int hashCode() {
        return first.hashCode();
    }
}
