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
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.common.io.ByteBufferInputStream;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Stream record format that interprets the body as avro encoded binary data.
 */
public class AvroRecordFormat extends StreamEventRecordFormat<GenericRecord> {
  private final ByteBufferInputStream byteBufferInput = new ByteBufferInputStream(ByteBuffers.EMPTY_BUFFER);
  private final DecoderFactory decoderFactory = DecoderFactory.get();

  private org.apache.avro.Schema readSchema;
  private String readSchemaHash;
  private String writeSchemaHash;
  private GenericDatumReader<GenericRecord> datumReader;
  private BinaryDecoder binaryDecoder;

  @Override
  public GenericRecord read(StreamEvent event) {
    try {
      // Check if the event has different schema then the read schema. If it does update the datumReader
      String eventSchemaStr = event.getHeaders().get(Constants.Stream.Headers.SCHEMA);
      if (eventSchemaStr != null) {
        String eventSchemaHash = event.getHeaders().get(Constants.Stream.Headers.SCHEMA_HASH);
        if (!this.writeSchemaHash.equals(eventSchemaHash)) {
          org.apache.avro.Schema writeSchema = new org.apache.avro.Schema.Parser().parse(eventSchemaStr);
          datumReader.setSchema(writeSchema);
          writeSchemaHash = eventSchemaHash;
        }
      } else {
        // If no schema is available on the event, assume it's the same as read schema
        datumReader.setSchema(readSchema);
        writeSchemaHash = readSchemaHash;
      }

      binaryDecoder = decoderFactory.binaryDecoder(byteBufferInput.reset(event.getBody()), binaryDecoder);
      return datumReader.read(null, binaryDecoder);
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
      readSchema = new org.apache.avro.Schema.Parser().parse(desiredSchema.toString());
    } catch (SchemaParseException e) {
      throw new UnsupportedTypeException("Schema is not a valid avro schema.", e);
    } catch (Exception e) {
      throw new UnsupportedTypeException("Exception parsing schema as an avro schema.", e);
    }
  }

  @Override
  protected void configure(Map<String, String> settings) {
    try {
      // Not using guava Hashing.md5() here to avoid potential version conflict issue when used in Hive
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      // Before actually reading any event, we assume the write schema is the same as the read schema
      readSchemaHash = Bytes.toHexString(md5.digest(Bytes.toBytes(readSchema.toString())));
      writeSchemaHash = readSchemaHash;
      datumReader = new GenericDatumReader<GenericRecord>(readSchema);
    } catch (NoSuchAlgorithmException e) {
      // This shouldn't happen.
      throw new RuntimeException(e);
    }
  }
}
