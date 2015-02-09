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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * Encodes an object as a {@link Put} for storing it into a {@link Table}. Assumes that objects to write are
 * records. Fields of simple types are encoded as columns in the table. Complex types (arrays, maps, records),
 * are binary encoded and stored as blobs.
 */
public class ReflectionPutWriter extends ReflectionWriter {
  private final Put put;

  public ReflectionPutWriter(Put put) {
    this.put = put;
  }

  @Override
  public void write(Object object, Schema objSchema) throws IOException {
    Preconditions.checkArgument(isRecord(objSchema), "Schema must be a record or nullable record.");
    super.writeRecord(null, object, objSchema);
  }

  @Override
  protected void writeNull(String name) throws IOException {
  }

  @Override
  protected void writeBool(String name, Boolean val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeInt(String name, int val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeLong(String name, long val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeFloat(String name, Float val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeDouble(String name, Double val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeString(String name, String val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeBytes(String name, ByteBuffer val) throws IOException {
    put.add(name, Bytes.toBytes(val));
  }

  @Override
  protected void writeBytes(String name, byte[] val) throws IOException {
    put.add(name, val);
  }

  @Override
  protected void writeEnum(String name, String val, Schema schema) throws IOException {
    int idx = schema.getEnumIndex(val);
    if (idx < 0) {
      throw new IOException("Invalid enum value " + val);
    }
    put.add(name, idx);
  }

  @Override
  protected void writeArray(String name, Collection val, Schema componentSchema) throws IOException {
    put.add(name, encodeArray(val, componentSchema));
  }

  @Override
  protected void writeArray(String name, Object val, Schema componentSchema) throws IOException {
    put.add(name, encodeArray(val, componentSchema));
  }

  @Override
  protected void writeMap(String name, Map<?, ?> val,
                          Map.Entry<Schema, Schema> mapSchema) throws IOException {
    put.add(name, encodeMap(val, mapSchema.getKey(), mapSchema.getValue()));
  }

  @Override
  protected void writeRecord(String name, Object record, Schema recordSchema) throws IOException {
    put.add(name, encodeRecord(record, recordSchema));
  }

  @Override
  protected void writeUnion(String name, Object val, Schema schema) throws IOException {
    // only support unions if its for a nullable.
    if (!schema.isNullable()) {
      throw new UnsupportedOperationException("Unions that do not represent nullables are not supported.");
    }

    if (val != null) {
      seenRefs.remove(val);
      write(name, val, schema.getNonNullable());
    }
  }

  private byte[] encodeArray(Object array, Schema componentSchema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(Schema.arrayOf(componentSchema));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(array, encoder);
    return bos.toByteArray();
  }

  private byte[] encodeMap(Map<?, ?> map, Schema keySchema, Schema valSchema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(Schema.mapOf(keySchema, valSchema));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(map, encoder);
    return bos.toByteArray();
  }

  private byte[] encodeRecord(Object record, Schema schema) throws IOException {
    ReflectionDatumWriter datumWriter = new ReflectionDatumWriter(schema);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bos);
    datumWriter.encode(record, encoder);
    return bos.toByteArray();
  }

  // return true if the schema is a record or a nullable record
  private boolean isRecord(Schema schema) {
    if (schema.isNullable()) {
      return schema.getNonNullable().getType() == Schema.Type.RECORD;
    } else {
      return schema.getType() == Schema.Type.RECORD;
    }
  }
}
