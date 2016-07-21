package com.packt.ch3.etl.geo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class GeoKey implements WritableComparable {
    private Text location;
    private FloatWritable latitude;
    private FloatWritable longitude;
    public GeoKey() {
        location = null;
        latitude = null;
        longitude = null;
    }

    public GeoKey(Text location, FloatWritable latitude, FloatWritable longitude) {
        this.location = location;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public FloatWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(FloatWritable latitude) {
        this.latitude = latitude;
    }

    public Text getLocation() {
        return location;
    }

    public void setLocation(Text location) {
        this.location = location;
    }

    public FloatWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(FloatWritable longitude) {
        this.longitude = longitude;
    }

    public void write(DataOutput d) throws IOException {
        location.write(d);
        latitude.write(d);
        longitude.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (location == null) {
            location = new Text();
        }
        if (latitude == null) {
            latitude = new FloatWritable();
        }
        if (longitude == null) {
            longitude = new FloatWritable();
        }
        location.readFields(di);
        latitude.readFields(di);
        longitude.readFields(di);
    }

    
    public int compareTo(Object o) {
        GeoKey other = (GeoKey)o;
        int cmp = location.compareTo(other.location);
        if (cmp != 0) {
            return cmp;
        }
        cmp = latitude.compareTo(other.latitude);
        if (cmp != 0) {
            return cmp;
        }
        return longitude.compareTo(other.longitude);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final GeoKey other = (GeoKey) obj;
        if (this.location != other.location && (this.location == null || !this.location.equals(other.location))) {
            return false;
        }
        if (this.latitude != other.latitude && (this.latitude == null || !this.latitude.equals(other.latitude))) {
            return false;
        }
        if (this.longitude != other.longitude && (this.longitude == null || !this.longitude.equals(other.longitude))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.location != null ? this.location.hashCode() : 0);
        hash = 97 * hash + (this.latitude != null ? this.latitude.hashCode() : 0);
        hash = 97 * hash + (this.longitude != null ? this.longitude.hashCode() : 0);
        return hash;
    }
    
    
}
