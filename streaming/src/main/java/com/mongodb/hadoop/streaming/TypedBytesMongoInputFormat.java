/*
 * Copyright 2011 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.streaming;

// Mongo

import com.mongodb.*;
import com.mongodb.hadoop.*;
import com.mongodb.hadoop.input.*;
import com.mongodb.hadoop.util.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.bson.*;

import java.io.*;
import java.util.*;

// Commons
// Hadoop
// Java

/**
* TypedBytesMongoInputFormat is a wrapper the TypedBytesMongoInputFormat that transforms all input/output into TypedBytes.
* This is done so Dumbo can use it.
*/
public class TypedBytesMongoInputFormat implements InputFormat<TypedBytesWritable, TypedBytesWritable> {    
    private static final Log LOG = LogFactory.getLog( TypedBytesMongoInputFormat.class );

    @Override
    public RecordReader<TypedBytesWritable, TypedBytesWritable> getRecordReader( 
        org.apache.hadoop.mapred.InputSplit split, 
        JobConf job, 
        Reporter reporter ) 
    {
        if ( !( split instanceof OldApiMongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a OldApiMongoInputSplit instance." );

        final OldApiMongoInputSplit mis = (OldApiMongoInputSplit) split;

        return new TypedBytesMongoRecordReader(mis.getBase());
    }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits( JobConf job, int numSplits ){
        final MongoConfig conf = new MongoConfig( job );
        List<org.apache.hadoop.mapreduce.InputSplit> list = MongoSplitter.calculateSplits( conf );
        org.apache.hadoop.mapred.InputSplit[] arr = new org.apache.hadoop.mapred.InputSplit[list.size()];
        for (int i = 0; i < list.size(); i++) {
            if (!(list.get(i) instanceof MongoInputSplit)) {
                throw new IllegalStateException("Calculating mongo splits should return a type of MongoInputSplit!");
            }
            arr[i] = new OldApiMongoInputSplit((MongoInputSplit) list.get(i));
        }
        return arr;
    }

    public class OldApiMongoInputSplit implements org.apache.hadoop.mapred.InputSplit {
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

    public class TypedBytesMongoRecordReader implements RecordReader<TypedBytesWritable, TypedBytesWritable> {
        MongoRecordReader reader;
        long i;

        public TypedBytesMongoRecordReader(MongoInputSplit split) {
            reader = new MongoRecordReader(split);
            reader.initialize(split, null);
            i = 0;
        }

        public void close() {
            reader.close();
        }

        public boolean next(TypedBytesWritable key, TypedBytesWritable val) {
            if (!reader.nextKeyValue())
                return false;
            i++;
            key.setValue(reader.getCurrentKey());
            val.setValue(reader.getCurrentValue());
            return true;
        }

        public TypedBytesWritable createKey() {
            return new TypedBytesWritable();
        }

        public TypedBytesWritable createValue() {
            return new TypedBytesWritable();
        }

        public long getPos() {
            return i;
        }

        public float getProgress() {
            return reader.getProgress();
        }
    }
}
