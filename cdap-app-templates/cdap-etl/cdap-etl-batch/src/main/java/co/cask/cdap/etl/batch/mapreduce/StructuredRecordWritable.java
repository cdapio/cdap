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
package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class StructuredRecordWritable implements WritableComparable<StructuredRecord> {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private StructuredRecord object;

  public StructuredRecordWritable() {
    this(null);
  }

  public StructuredRecordWritable(StructuredRecord object) {
    this.object = object;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    byte[] schema = GSON.toJson(object.getSchema()).getBytes(Charsets.UTF_8);
    dataOutput.write(schema.length);
    dataOutput.write(schema);

    byte[] record = StructuredRecordStringConverter.toJsonString(object).getBytes(Charsets.UTF_8);
    dataOutput.write(record.length);
    dataOutput.write(record);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int schemaLen = dataInput.readInt();
    byte[] schemaBytes = new byte[schemaLen];
    dataInput.readFully(schemaBytes, 0, schemaLen);
    Schema schema = GSON.fromJson(new String(schemaBytes, Charsets.UTF_8), Schema.class);

    int recordLen = dataInput.readInt();
    byte[] recordBytes = new byte[recordLen];
    dataInput.readFully(recordBytes, 0, recordLen);
    String recordJson = new String(recordBytes, Charsets.UTF_8);

    this.object = StructuredRecordStringConverter.fromJsonString(recordJson, schema);
  }

  public StructuredRecord getObject() {
    return object;
  }

  @Override
  public int compareTo(StructuredRecord o) {
    return 0;
  }
}
