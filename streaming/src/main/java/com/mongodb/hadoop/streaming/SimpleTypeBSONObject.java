package com.mongodb.hadoop.streaming;

import org.bson.*;
import org.bson.types.*;

/**
* A Converter for BSON objects to TypedByte-able formats
*
* Use: TypedBytesWritable tbw; tbw.setValue(SimpleTypeBSONObject.convert(bobj));
*/
public class SimpleTypeBSONObject {

    public static Object convert(Object obj) {
        return typedByteable(obj);
    }

    public static class TypedBytesBSONMap extends BasicBSONObject {
        public TypedBytesBSONMap(BSONObject bobj) {
            super(bobj.toMap());
        }

        @Override
        public Object get(String key) {
            return typedByteable(super.get(key));
        }
    }

    public static class TypedBytesBSONList extends BasicBSONList {
        public TypedBytesBSONList(BasicBSONList blist) {
            super();
            for (int i = 0; i < blist.size(); i++) {
                super.put(i, typedByteable(blist.get(i)));
            }
        }
    }

    private static Object typedByteable(Object obj) {
        if (obj instanceof BasicBSONList) {
            return new TypedBytesBSONList((BasicBSONList) obj);       
        } else if (obj instanceof ObjectId) {
            return ((ObjectId) obj).toString();
        } else if (obj instanceof BSONObject) {
            return new TypedBytesBSONMap((BSONObject) obj);
        } else {
            return obj;
        }
    }
}