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
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.bson.*;

import java.util.*;

// Commons
// Hadoop
// Java

/**
* TypedBytesMongoInputFormat is a wrapper the TypedBytesMongoInputFormat that transforms all input/output into TypedBytes.
* This is done so Dumbo can use it.
*/
public class TypedBytesMongoInputFormat implements org.apache.hadoop.mapred.InputFormat<TypedBytesWritable, TypedBytesWritable> {    
    private static final Log LOG = LogFactory.getLog( TypedBytesMongoInputFormat.class );

    public RecordReader<TypedBytesWritable, TypedBytesWritable> getRecordReader( InputSplit split, JobConf job, Reporter reporter ){
        if ( !( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a MongoInputSplit instance." );

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new TypedBytesMongoRecordReader(mis);
    }

    public InputSplit[] getSplits( JobConf job, int numSplits ){
        final MongoConfig conf = new MongoConfig( job );
        return (InputSplit[]) MongoSplitter.calculateSplits( conf ).toArray();
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
