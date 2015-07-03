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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.common.io.ByteBufferInputStream;
import org.apache.avro.SchemaParseException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Byte Buffer record format that interprets the body as avro encoded binary data (used with Kafka events).
 */
public class AvroRecordFormat extends KafkaEventRecord<StructuredRecord> {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final ByteBufferInputStream byteBufferInput = new ByteBufferInputStream(EMPTY_BUFFER);
  private final DecoderFactory decoderFactory = DecoderFactory.get();

  private Schema formatSchema;
  private org.apache.avro.Schema avroFormatSchema;

  private StructuredRecordDatumReader datumReader;
  private BinaryDecoder binaryDecoder;

  @Override
  public StructuredRecord read(ByteBuffer byteBuffer) {
    try {
      datumReader.setSchema(avroFormatSchema);
      binaryDecoder = decoderFactory.binaryDecoder(byteBufferInput.reset(byteBuffer), binaryDecoder);
      return datumReader.read(null, binaryDecoder);
    } catch (IOException e) {
      throw new UnexpectedFormatException("Unable to decode the kafka message payload as avro.", e);
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
      avroFormatSchema = new org.apache.avro.Schema.Parser().parse(desiredSchema.toString());
      formatSchema = desiredSchema;
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
      // Before actually reading any event, we assume the event schema is the same as the format schema
      datumReader = new StructuredRecordDatumReader(formatSchema, avroFormatSchema);
    } catch (NoSuchAlgorithmException e) {
      // This shouldn't happen.
      throw new RuntimeException(e);
    }
  }
}
