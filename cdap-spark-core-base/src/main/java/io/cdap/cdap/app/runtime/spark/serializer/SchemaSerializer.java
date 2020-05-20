/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Kryo {@link Serializer} for {@link Schema}.
 */
public class SchemaSerializer extends Serializer<Schema> {

  @Override
  public void write(Kryo kryo, Output output, Schema schema) {
    writeSchema(kryo, output, schema, new HashSet<>());
  }

  @Override
  public Schema read(Kryo kryo, Input input, Class<Schema> cls) {
    Schema.Type type = kryo.readObject(input, Schema.Type.class);
    Schema.LogicalType logicalType = kryo.readObjectOrNull(input, Schema.LogicalType.class);

    if (type.isSimpleType()) {
      return logicalType == null ? Schema.of(type) : Schema.of(logicalType);
    }

    switch (type) {
      case ENUM:
        return Schema.enumWith((List<String>) kryo.readObject(input, ArrayList.class));
      case ARRAY:
        return Schema.arrayOf(read(kryo, input, Schema.class));
      case MAP:
        return Schema.mapOf(read(kryo, input, Schema.class), read(kryo, input, Schema.class));
      case RECORD:
        String recordName = input.readString();
        int fieldSize = input.readInt();
        List<Schema.Field> fields = new ArrayList<>(fieldSize);
        for (int i = 0; i < fieldSize; i++) {
          fields.add(Schema.Field.of(input.readString(), read(kryo, input, Schema.class)));
        }
        return fields.isEmpty() ? Schema.recordOf(recordName) : Schema.recordOf(recordName, fields);
      case UNION:
        int unionSize = input.readInt();
        List<Schema> schemas = new ArrayList<>(unionSize);
        for (int i = 0; i < unionSize; i++) {
          schemas.add(read(kryo, input, Schema.class));
        }
        return Schema.unionOf(schemas);
    }
    throw new KryoException("Failed to deserialize schema of unsupported type " + type);
  }

  /**
   * Serialize the given {@link Schema} object.
   *
   * @param kryo the {@link Kryo} object
   * @param output the output to write to
   * @param schema the schema object to be serialized
   * @param knownRecords a set of known record names to support recursive schema structure
   */
  private void writeSchema(Kryo kryo, Output output, Schema schema, Set<String> knownRecords) {
    Schema.Type type = schema.getType();
    kryo.writeObject(output, type);
    kryo.writeObjectOrNull(output, schema.getLogicalType(), Schema.LogicalType.class);

    switch (type) {
      case ENUM:
        kryo.writeObject(output, new ArrayList<>(schema.getEnumValues()));
        break;
      case ARRAY:
        writeSchema(kryo, output, schema.getComponentSchema(), knownRecords);
        break;
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        writeSchema(kryo, output, mapSchema.getKey(), knownRecords);
        writeSchema(kryo, output, mapSchema.getValue(), knownRecords);
        break;
      case RECORD:
        output.writeString(schema.getRecordName());
        if (knownRecords.add(schema.getRecordName())) {
          List<Schema.Field> fields = schema.getFields();
          output.writeInt(fields.size());
          for (Schema.Field field : fields) {
            output.writeString(field.getName());
            writeSchema(kryo, output, field.getSchema(), knownRecords);
          }
        } else {
          output.writeInt(0);
        }
        break;
      case UNION:
        List<Schema> schemas = schema.getUnionSchemas();
        output.writeInt(schemas.size());
        for (Schema s : schemas) {
          writeSchema(kryo, output, s, knownRecords);
        }
        break;
    }
  }
}
