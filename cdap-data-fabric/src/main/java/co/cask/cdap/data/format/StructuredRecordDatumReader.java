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

package co.cask.cdap.data.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

import java.io.IOException;
import java.util.Collection;

/**
 * An Avro {@link DatumReader} that reads data into {@link StructuredRecord}.
 */
public class StructuredRecordDatumReader extends GenericDatumReader<StructuredRecord> {

  private Schema currentSchema;

  /**
   * Constructs a new instance.
   *
   * @param schema The schema for the {@link StructuredRecord} created through this class
   * @param avroSchema The Avro Schema equivalent of the schema
   */
  public StructuredRecordDatumReader(Schema schema, org.apache.avro.Schema avroSchema) {
    this.currentSchema = schema;
    setExpected(avroSchema);
  }

  @Override
  protected Object read(Object old, org.apache.avro.Schema expected, ResolvingDecoder in) throws IOException {
    if (expected.getType() != org.apache.avro.Schema.Type.UNION) {
      return super.read(old, expected, in);
    }

    // For Union type
    Schema tmpSchema = currentSchema;
    try {
      int idx = in.readIndex();
      currentSchema = currentSchema.getUnionSchema(idx);
      return read(old, expected.getTypes().get(idx), in);
    } finally {
      currentSchema = tmpSchema;
    }
  }

  @Override
  protected Object newArray(Object old, int size, org.apache.avro.Schema schema) {
    if (old instanceof Collection) {
      ((Collection) old).clear();
      return old;
    } else {
      return Lists.newArrayListWithExpectedSize(size);
    }
  }

  @Override
  protected Object createEnum(String symbol, org.apache.avro.Schema schema) {
    return symbol;
  }

  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    // super.readString returns an Utf8 object. Calling toString() to turn it into a String.
    return super.readString(old, in).toString();
  }

  @Override
  protected Object readArray(Object old, org.apache.avro.Schema expected, ResolvingDecoder in) throws IOException {
    Schema tmpSchema = currentSchema;
    try {
      currentSchema = currentSchema.getComponentSchema();
      return super.readArray(old, expected, in);
    } finally {
      currentSchema = tmpSchema;
    }
  }

  @Override
  protected Object readMap(Object old, org.apache.avro.Schema expected, ResolvingDecoder in) throws IOException {
    Schema tmpSchema = currentSchema;
    try {
      currentSchema = tmpSchema.getMapSchema().getValue();
      return super.readMap(old, expected, in);
    } finally {
      currentSchema = tmpSchema;
    }
  }

  @Override
  protected Object readRecord(Object old, org.apache.avro.Schema expected, ResolvingDecoder in) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(currentSchema);

    for (org.apache.avro.Schema.Field f : in.readFieldOrder()) {
      String name = f.name();
      Schema tmpSchema = currentSchema;
      try {
        currentSchema = getFieldSchema(name, currentSchema);
        builder.set(name, read(null, f.schema(), in));
      } finally {
        currentSchema = tmpSchema;
      }
    }
    return builder.build();
  }

  /**
   * Returns the {@link Schema} of the given field in the record.
   *
   * @throws IllegalArgumentException if the field does not exist in the record schema.
   */
  private Schema getFieldSchema(String fieldName, Schema recordSchema) {
    Schema.Field field = recordSchema.getField(fieldName);
    if (field == null) {
      throw new IllegalArgumentException("Field '" + fieldName + "' not exists in record '" + recordSchema + "'");
    }
    return field.getSchema();
  }
}
