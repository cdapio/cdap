/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to convert between a Writable, specifically a MapWritable, and a {@link StructuredRecord}.
 */
public final class RecordWritableConverter {
  public static MapWritable covertToWritable(StructuredRecord record) throws IOException {
    MapWritable result = new MapWritable();
    for (Schema.Field field : record.getSchema().getFields()) {
      try {
        result.put(new Text(field.getName()), getWritables(record.get(field.getName()), field.getSchema()));
      } catch (Exception e) {
        throw(new IOException(String.format("Type exception for field %s: %s",
                                            field.getName(), e.getMessage())));
      }
    }
    return result;
  }

  private static Writable getWritables(Object object, Schema schema) throws IOException {
    if (object == null && schema.getType() != Schema.Type.NULL && schema.getType() != Schema.Type.UNION) {
      throw new ClassCastException("This object is null.");
    }
    switch (schema.getType()) {
      case NULL:
        if (object == null) {
          return NullWritable.get();
        }
        throw new ClassCastException("This object is not null: " + object.toString());
      case BOOLEAN:
        return new BooleanWritable((boolean) object);
      case INT:
        return new IntWritable((int) object);
      case LONG:
        return new LongWritable((long) object);
      case FLOAT:
        return new FloatWritable((float) object);
      case DOUBLE:
        return new DoubleWritable((double) object);
      case BYTES:
        return new BytesWritable((byte[]) object);
      case STRING:
        return new Text((String) object);
      case ENUM:
        // Currently there is no standard container to represent enum type
        return new Text((String) object);
      case ARRAY:
        return convertFromArray((ArrayList) object, schema.getComponentSchema());
      case MAP:
        return convertFromMap((Map) object, schema.getMapSchema());
      case RECORD:
        return covertToWritable((StructuredRecord) object);
      case UNION:
        return convertFromUnion(object, schema.getUnionSchemas());
    }
    throw new IOException("Unsupported schema: " + schema.getType());
  }

  private static ArrayWritable convertFromArray(ArrayList list, Schema schema) throws IOException {
    Writable[] writableArray = new Writable[list.size()];
    for (int i = 0; i < list.size(); i++) {
      writableArray[i] = getWritables(list.get(i), schema);
    }
    return new ArrayWritable(Writable.class, writableArray);
  }

  private static Writable convertFromUnion(Object object, List<Schema> schemas) throws IOException {
    for (Schema schema : schemas) {
      try {
        return getWritables(object, schema);
      } catch (Exception e) {
        //no-op; we expect failed class conversions
      }
    }
    throw new IOException("Object " + object.toString() + " is not of correct type");
  }

  private static MapWritable convertFromMap(Map map, Map.Entry<Schema, Schema> schemaMap) throws IOException {
    MapWritable mapWritable = new MapWritable();
    for (Object key : map.keySet()) {
      mapWritable.put(getWritables(key, schemaMap.getKey()), getWritables(map.get(key), schemaMap.getValue()));
    }
    return mapWritable;
  }

  public static StructuredRecord convertToRecord(MapWritable input, Schema schema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    for (Schema.Field field : schema.getFields()) {
      try {
        builder.set(field.getName(), convertWritables(input.get(new Text(field.getName())), field.getSchema()));
      } catch (Exception e) {
        throw(new IOException(String.format("Type exception for field %s: %s",
                                            field.getName(), e.getMessage())));
      }
    }
    return builder.build();
  }

  private static Object convertWritables(Writable writable, Schema schema) throws IOException {
    if (writable.getClass() == NullWritable.class && schema.getType() != Schema.Type.NULL
      && schema.getType() != Schema.Type.UNION) {
      throw new ClassCastException("This field is null.");
    }
    switch (schema.getType()) {
      case NULL:
        if (writable.getClass() == NullWritable.class) {
          return null;
        }
        throw new ClassCastException("This field is not null:" + writable.toString());
      case BOOLEAN:
        return ((BooleanWritable) writable).get();
      case INT:
        //Downcasting is necessary because Elasticsearch defaults to storing all ints as longs
        return (int) (writable.getClass() == IntWritable.class ? ((IntWritable) writable).get() :
          ((LongWritable) writable).get());
      case LONG:
        return ((LongWritable) writable).get();
      case FLOAT:
        // Downcasting is necessary because Elasticsearch defaults to storing all floats as doubles
        return (float) (writable.getClass() == FloatWritable.class ? ((FloatWritable) writable).get() :
          ((DoubleWritable) writable).get());
      case DOUBLE:
        return ((DoubleWritable) writable).get();
      case BYTES:
        return ((BytesWritable) writable).getBytes();
      case STRING:
        return writable.toString();
      case ENUM:
        // Currently there is no standard container to represent enum type
        return writable.toString();
      case ARRAY:
        return convertArray((ArrayWritable) writable, schema.getComponentSchema());
      case MAP:
        return convertMap((MapWritable) writable, schema.getMapSchema());
      case RECORD:
        return convertToRecord((MapWritable) writable, schema);
      case UNION:
        return convertUnion(writable, schema);
    }
    throw new IOException("Unsupported schema: " + schema);
  }

  private static List<Object> convertArray(ArrayWritable input, Schema elementSchema) throws IOException {
    List<Object> result = new ArrayList<>();
    for (Writable writable : input.get()) {
      result.add(convertWritables(writable, elementSchema));
    }
    return result;
  }

  private static Map<Object, Object> convertMap(MapWritable input,
                                                Map.Entry<Schema, Schema> mapSchema) throws IOException {
    Schema keySchema = mapSchema.getKey();
    if (!keySchema.isCompatible(Schema.of(Schema.Type.STRING))) {
      throw new IOException("Complex key type not supported: " + keySchema);
    }

    Schema valueSchema = mapSchema.getValue();
    Map<Object, Object> result = new HashMap<>();

    for (Writable key : input.keySet()) {
      result.put(convertWritables(key, keySchema), convertWritables(input.get(key), valueSchema));
    }
    return result;
  }

  private static Object convertUnion(Writable input, Schema unionSchema) throws IOException {
    for (Schema schema : unionSchema.getUnionSchemas()) {
      try {
        return convertWritables(input, schema);
      } catch (ClassCastException e) {
        //no-op; keep iterating until the appropriate class is found
      }
    }
    throw new IOException("No matching schema found for union type: " + unionSchema);
  }

  private RecordWritableConverter() {
    //no-op for static class
  }
}
