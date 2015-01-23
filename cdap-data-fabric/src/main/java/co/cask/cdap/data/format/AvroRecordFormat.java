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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Stream record format that interprets the body as avro encoded binary data.
 */
public class AvroRecordFormat extends ByteBufferRecordFormat<GenericRecord> {
  private org.apache.avro.Schema avroSchema;
  private DecoderFactory decoderFactory = DecoderFactory.get();
  private DatumReader<GenericRecord> datumReader;
  private BinaryDecoder binaryDecoder;

  @Override
  public GenericRecord read(ByteBuffer input) {
    try {
      // TODO: CDAP-1199 look at reusing decoder and generic record
      return datumReader.read(null, decoderFactory.binaryDecoder(Bytes.toBytes(input), binaryDecoder));
    } catch (IOException e) {
      throw new UnexpectedFormatException("Unable to decode the stream body as avro.", e);
    }
  }

  @Override
  protected Schema getDefaultSchema() {
    return null;
  }

  @Override
  protected void validateSchema(Schema desiredSchema) throws UnsupportedTypeException {
    try {
      // rather than check for all inconsistencies, just try to read the schema string as an Avro schema.
      avroSchema = new org.apache.avro.Schema.Parser().parse(desiredSchema.toString());
    } catch (SchemaParseException e) {
      throw new UnsupportedTypeException("Schema is not a valid avro schema.", e);
    } catch (Exception e) {
      throw new UnsupportedTypeException("Exception parsing schema as an avro schema.", e);
    }
  }

  @Override
  protected void configure(Map<String, String> settings) {
    datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
    binaryDecoder = decoderFactory.binaryDecoder((InputStream) null, null);
  }
}
