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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaCache;
import io.cdap.cdap.format.io.StructuredRecordDatumReader;
import io.cdap.cdap.format.io.StructuredRecordDatumWriter;

import java.io.IOException;

/**
 * A Kryo {@link Serializer} for {@link StructuredRecord}.
 */
public class StructuredRecordSerializer extends Serializer<StructuredRecord> {

  private static final StructuredRecordDatumWriter DATUM_WRITER = new StructuredRecordDatumWriter();
  private static final StructuredRecordDatumReader DATUM_READER = new StructuredRecordDatumReader();

  @Override
  public void write(Kryo kryo, Output output, StructuredRecord record) {
    // First write out the schema as two fields: hash and json representation
    // Later with the cache we may skip deserializing JSON if schema is present in cache
    kryo.writeObject(output, record.getSchema().getSchemaHash().toString());
    kryo.writeObject(output, record.getSchema().toString());

    // Then write out the data
    try {
      DATUM_WRITER.encode(record, new KryoEncoder(output));
    } catch (IOException e) {
      throw new KryoException("Failed to encode StructuredRecord " + record.getSchema().getRecordName(), e);
    }
  }

  @Override
  public StructuredRecord read(Kryo kryo, Input input, Class<StructuredRecord> type) {
    // Read the schema
    String schemaHashStr = kryo.readObject(input, String.class);
    String schemaJson = kryo.readObject(input, String.class);
    Schema schema = SchemaCache.fromJson(schemaHashStr, schemaJson);
    try {
      return DATUM_READER.read(new KryoDecoder(input), schema);
    } catch (IOException e) {
      throw new KryoException("Failed to decode StructuredRecord " + schema.getRecordName(), e);
    }
  }
}
