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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.io.BinaryEncoder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Encodes an object as a {@link Put} for storing it into a {@link Table}. Assumes that objects to write are
 * records. Fields of simple types are encoded as columns in the table. Complex types (arrays, maps, records),
 * are binary encoded and stored as blobs.
 *
 * @param <T> the type of object to encode as a {@link Put}
 */
public class ReflectionPutWriter<T> extends ReflectionWriter<Put, T> {
  private final List<String> fieldNames;
  private int index;

  public ReflectionPutWriter(Schema schema) {
    super(schema);
    Preconditions.checkArgument(schema.getType() == Schema.Type.RECORD, "Schema must be a record.");
    List<Schema.Field> schemaFields = schema.getFields();
    int numFields = schemaFields.size();
    Preconditions.checkArgument(numFields > 0, "Record must contain at least one field.");
    this.fieldNames = Lists.newArrayListWithCapacity(numFields);
    for (Schema.Field schemaField : schemaFields) {
      this.fieldNames.add(schemaField.getName());
    }
    this.index = 0;
  }

  private String nextField() {
    String name = fieldNames.get(index);
    index++;
    return name;
  }

  @Override
  public void write(T object, Put put) throws IOException {
    index = 0;
    seenRefs = Sets.newIdentityHashSet();
    super.writeRecord(put, object, schema);
  }

  @Override
  protected void writeNull(Put put) throws IOException {
    nextField();
  }

  @Override
  protected void writeBool(Put put, Boolean val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeInt(Put put, int val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeLong(Put put, long val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeFloat(Put put, Float val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeDouble(Put put, Double val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeString(Put put, String val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeBytes(Put put, ByteBuffer val) throws IOException {
    put.add(nextField(), Bytes.toBytes(val));
  }

  @Override
  protected void writeBytes(Put put, byte[] val) throws IOException {
    put.add(nextField(), val);
  }

  @Override
  protected void writeEnum(Put put, String val, Schema schema) throws IOException {
    int idx = schema.getEnumIndex(val);
    if (idx < 0) {
      throw new IOException("Invalid enum value " + val);
    }
    put.add(nextField(), idx);
  }

  @Override
  protected void writeArray(Put put, Collection val, Schema componentSchema) throws IOException {
    put.add(nextField(), encodeArray(val, componentSchema));
  }

  @Override
  protected void writeArray(Put put, Object val, Schema componentSchema) throws IOException {
    put.add(nextField(), encodeArray(val, componentSchema));
  }

  @Override
  protected void writeMap(Put put, Map<?, ?> val, Map.Entry<Schema, Schema> mapSchema) throws IOException {
    put.add(nextField(), encodeMap(val, mapSchema.getKey(), mapSchema.getValue()));
  }

  @Override
  protected void writeRecord(Put put, Object record, Schema recordSchema) throws IOException {
    put.add(nextField(), encodeRecord(record, recordSchema));
  }

  @Override
  protected void writeUnion(Put put, Object val, Schema schema) throws IOException {
    // only support unions if its for a nullable.
    if (!schema.isNullable()) {
      throw new UnsupportedOperationException("Unions that do not represent nullables are not supported.");
    }

    if (val != null) {
      seenRefs.remove(val);
      write(put, val, schema.getNonNullable());
    }
  }

  @SuppressWarnings("unchecked")
  private byte[] encodeArray(Object array, Schema componentSchema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(Schema.arrayOf(componentSchema));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(array, encoder);
    return bos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private byte[] encodeMap(Map<?, ?> map, Schema keySchema, Schema valSchema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(Schema.mapOf(keySchema, valSchema));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(map, encoder);
    return bos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private byte[] encodeRecord(Object record, Schema schema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(schema);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(record, encoder);
    return bos.toByteArray();
  }
}
