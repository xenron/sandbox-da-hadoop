package com.packt.ch3.etl.geo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class GeoValue implements WritableComparable {
    private Text eventDate;
    private Text eventType;
    private Text actor;
    private Text source;
    private IntWritable fatalities;
    
    public GeoValue() {
        eventDate = null;
        eventType = null;
        actor = null;
        source = null;
        fatalities = null;
    }

    public Text getActor() {
        return actor;
    }

    public void setActor(Text actor) {
        this.actor = actor;
    }

    public Text getEventDate() {
        return eventDate;
    }

    public void setEventDate(Text eventDate) {
        this.eventDate = eventDate;
    }

    public Text getEventType() {
        return eventType;
    }

    public void setEventType(Text eventType) {
        this.eventType = eventType;
    }

    public IntWritable getFatalities() {
        return fatalities;
    }

    public void setFatalities(IntWritable fatalities) {
        this.fatalities = fatalities;
    }

    public Text getSource() {
        return source;
    }

    public void setSource(Text source) {
        this.source = source;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GeoValue other = (GeoValue) obj;
        if (this.eventDate != other.eventDate && (this.eventDate == null || !this.eventDate.equals(other.eventDate))) {
            return false;
        }
        if (this.actor != other.actor && (this.actor == null || !this.actor.equals(other.actor))) {
            return false;
        }
        if (this.source != other.source && (this.source == null || !this.source.equals(other.source))) {
            return false;
        }
        if (this.fatalities != other.fatalities && (this.fatalities == null || !this.fatalities.equals(other.fatalities))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 29 * hash + (this.eventDate != null ? this.eventDate.hashCode() : 0);
        hash = 29 * hash + (this.eventType != null ? this.eventType.hashCode() : 0);
        hash = 29 * hash + (this.actor != null ? this.actor.hashCode() : 0);
        hash = 29 * hash + (this.source != null ? this.source.hashCode() : 0);
        hash = 29 * hash + (this.fatalities != null ? this.fatalities.hashCode() : 0);
        return hash;
    }
    

    public void write(DataOutput d) throws IOException {
        eventDate.write(d);
        eventType.write(d);
        actor.write(d);
        source.write(d);
        fatalities.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (eventDate == null) {
            eventDate = new Text();
        }
        if (eventType == null) {
            eventType = new Text();
        }
        if (actor == null) {
            actor = new Text();
        }
        if (source == null) {
            source = new Text();
        }
        if (fatalities == null) {
            fatalities = new IntWritable();
        }
        eventDate.readFields(di);
        eventType.readFields(di);
        actor.readFields(di);
        source.readFields(di);
        fatalities.readFields(di);
    }

    public int compareTo(Object o) {
        GeoValue other = (GeoValue)o;
        int cmp = eventDate.compareTo(other.eventDate);
        if (cmp != 0) {
            return cmp;
        }
        cmp = eventType.compareTo(other.eventType);
        if (cmp != 0) {
            return cmp;
        }
        cmp = actor.compareTo(other.actor);
        if (cmp != 0) {
            return cmp;
        }
        cmp = source.compareTo(other.source);
        if (cmp != 0) {
            return cmp;
        }
        return fatalities.compareTo(other.fatalities);
    }
    
    
}
