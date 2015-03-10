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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.common.Bytes;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A helper to deal with Avro schema, records, serializations, etc.
 * This is in its own class because it uses Avro the Schema class, which would clash with our own Schema elsewhere.
 */
public class FileWriterHelper {

  /**
   * Generate an Avro file of schema (key String, value String) containing the records ("<prefix>i", "#i")
   * for start <= i < end. The file is written using the passed-in output stream.
   */
  public static void generateAvroFile(OutputStream out, String prefix, int start, int end) throws IOException {
    Schema schema = Schema.createRecord("kv", null, null, false);
    schema.setFields(ImmutableList.of(
      new Schema.Field("key", Schema.create(Schema.Type.STRING), null, null),
      new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, out);
    try {
      for (int i = start; i < end; i++) {
        GenericRecord kv = new GenericData.Record(schema);
        kv.put("key", prefix + i);
        kv.put("value", "#" + i);
        dataFileWriter.append(kv);
      }
    } finally {
      Closeables.closeQuietly(dataFileWriter);
      Closeables.closeQuietly(out);
    }
  }

  /**
   * Generate a text file of schema (key String, value Int) containing the records ("<prefix>i", i)
   * for start <= i < end, using the given delimiter. The file is written using the passed-in output stream.
   */
  public static void generateTextFile(OutputStream out, String delim, String prefix, int start, int end)
    throws IOException {
    try {
      for (int i = start; i < end; i++) {
        String line = String.format("%s%d%s%d\n", prefix, i, delim, i);
        out.write(Bytes.toBytes(line));
      }
    } finally {
      Closeables.closeQuietly(out);
    }
  }

}
