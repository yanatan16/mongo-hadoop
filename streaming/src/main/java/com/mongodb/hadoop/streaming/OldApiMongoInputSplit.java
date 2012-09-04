package com.mongodb.hadoop.streaming;

import org.apache.hadoop.mapred.InputSplit;
import com.mongodb.hadoop.input.MongoInputSplit;
import java.io.*;

public class OldApiMongoInputSplit implements InputSplit {
    MongoInputSplit base;
    public OldApiMongoInputSplit(MongoInputSplit split) {
        base = split;
    }
    public long getLength() {
        return base.getLength();
    }
    public String[] getLocations() {
        return base.getLocations();
    }
    public void readFields( DataInput in ) throws IOException {
        base.readFields(in);
    }
    public void write (final DataOutput out) throws IOException {
        base.write(out);
    }
    public MongoInputSplit getBase() {
        return base;
    }
}