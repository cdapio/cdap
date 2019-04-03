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

package co.cask.cdap.app.runtime.spark.serializer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.io.StructuredRecordDatumReader;
import co.cask.cdap.format.io.StructuredRecordDatumWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

/**
 * A Kryo {@link Serializer} for {@link StructuredRecord}.
 */
public class StructuredRecordSerializer extends Serializer<StructuredRecord> {

  private static final StructuredRecordDatumWriter DATUM_WRITER = new StructuredRecordDatumWriter();
  private static final StructuredRecordDatumReader DATUM_READER = new StructuredRecordDatumReader();

  @Override
  public void write(Kryo kryo, Output output, StructuredRecord record) {
    // First write out the schema
    kryo.writeObject(output, record.getSchema());
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
    Schema schema = kryo.readObject(input, Schema.class);
    try {
      return DATUM_READER.read(new KryoDecoder(input), schema);
    } catch (IOException e) {
      throw new KryoException("Failed to decode StructuredRecord " + schema.getRecordName(), e);
    }
  }
}
