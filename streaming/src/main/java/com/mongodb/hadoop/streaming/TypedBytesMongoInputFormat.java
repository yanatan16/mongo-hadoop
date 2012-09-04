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
import org.apache.hadoop.mapreduce.*;
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
public class TypedBytesMongoInputFormat extends InputFormat<TypedBytesWritable, TypedBytesWritable> {    
    private static final Log LOG = LogFactory.getLog( TypedBytesMongoInputFormat.class );

    @Override
    public RecordReader<TypedBytesWritable, TypedBytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context ){
        if ( !( split instanceof MongoInputSplit ) )
            throw new IllegalStateException( "Creation of a new RecordReader requires a MongoInputSplit instance." );

        final MongoInputSplit mis = (MongoInputSplit) split;

        return new TypedBytesMongoRecordReader(mis);
    }

    @Override
    public List<InputSplit> getSplits( JobContext context ){
        final Configuration hadoopConfiguration = context.getConfiguration();
        final MongoConfig conf = new MongoConfig( hadoopConfiguration );
        return MongoSplitter.calculateSplits( conf );
    }

    public class TypedBytesMongoRecordReader extends RecordReader<TypedBytesWritable, TypedBytesWritable> {
        private MongoRecordReader reader;

        public TypedBytesMongoRecordReader(MongoInputSplit split) {
            reader = new MongoRecordReader(split);
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() {
            return reader.nextKeyValue();
        }

        @Override
        public TypedBytesWritable getCurrentKey() {
            TypedBytesWritable tbw = new TypedBytesWritable();
            tbw.setValue(reader.getCurrentKey());
            return tbw;
        }

        @Override
        public TypedBytesWritable getCurrentValue() {
            TypedBytesWritable tbw = new TypedBytesWritable();
            tbw.setValue(reader.getCurrentValue());
            return tbw;
        }

        @Override
        public float getProgress() {
            return reader.getProgress();
        }

        @Override
        public void close() {
            reader.close();
        }
    }
}
