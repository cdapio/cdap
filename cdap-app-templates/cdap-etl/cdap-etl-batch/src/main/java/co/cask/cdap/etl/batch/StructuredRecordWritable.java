/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Wrapper around a {@link StructuredRecord} so that it can be used as the output key and/or value of a mapper.
 * This is not very performant and must be improved soon (CDAP-5347).
 */
public class StructuredRecordWritable implements WritableComparable<StructuredRecordWritable> {
  // schema cache so that we do not parse schema string for each incoming record
  private static final Map<byte[], Schema> schemaCache = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private StructuredRecord record;

  // required by Hadoop
  @SuppressWarnings("unused")
  public StructuredRecordWritable() {
  }

  public StructuredRecordWritable(StructuredRecord record) {
    this.record = record;
  }

  public void set(StructuredRecord record) {
    this.record = record;
  }

  public StructuredRecord get() {
    return record;
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void write(DataOutput out) throws IOException {
    byte[] schemaBytes = Bytes.toBytes(record.getSchema().toString());
    out.writeInt(schemaBytes.length);
    out.write(schemaBytes);

    byte[] recordBytes = Bytes.toBytes(StructuredRecordStringConverter.toJsonString(record));
    out.writeInt(recordBytes.length);
    out.write(recordBytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int schemaLen = in.readInt();
    byte[] schemaBytes = new byte[schemaLen];
    in.readFully(schemaBytes, 0, schemaLen);

    Schema schema;
    if (schemaCache.containsKey(schemaBytes)) {
      schema = schemaCache.get(schemaBytes);
    } else {
      String schemaStr = Bytes.toString(schemaBytes);
      schema = Schema.parseJson(schemaStr);
      schemaCache.put(schemaBytes, schema);
    }

    int recordLen = in.readInt();
    byte[] recordBytes = new byte[recordLen];
    in.readFully(recordBytes, 0, recordLen);
    String recordStr = Bytes.toString(recordBytes);
    this.record = StructuredRecordStringConverter.fromJsonString(recordStr, schema);
  }

  @Override
  public int compareTo(StructuredRecordWritable o) {
    return Integer.compare(hashCode(), o.hashCode());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StructuredRecordWritable that = (StructuredRecordWritable) o;

    return Objects.equals(record, that.record);
  }

  @Override
  public int hashCode() {
    return record != null ? record.hashCode() : 0;
  }
}
