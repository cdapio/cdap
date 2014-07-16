/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.logging.appender.kafka;

import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.logging.serialize.LoggingEvent;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Throwables;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Avro serializer for ILoggingEvent.
 */
public final class LoggingEventSerializer {
  private final LogSchema logSchema;

  public LoggingEventSerializer() throws IOException {
    this.logSchema = new LogSchema();
  }

  public LoggingEventSerializer(VerifiableProperties props) throws IOException {
    this();
  }

  public Schema getAvroSchema() {
    return logSchema.getAvroSchema();
  }

  public byte[] toBytes(ILoggingEvent loggingEvent) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(logSchema.getAvroSchema());
    try {
      writer.write(LoggingEvent.encode(logSchema.getAvroSchema(), loggingEvent), encoder);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return out.toByteArray();
  }

  public ILoggingEvent fromBytes(ByteBuffer buffer) {
    return LoggingEvent.decode(toGenericRecord(buffer));
  }

  public GenericRecord toGenericRecord(ByteBuffer buffer) {
    ByteArrayInputStream in;
    if (buffer.hasArray()) {
      in = new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), buffer.limit());
    } else {
      byte [] bytes = new byte[buffer.limit()];
      buffer.get(bytes);
      in = new ByteArrayInputStream(bytes);
    }

    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(logSchema.getAvroSchema());
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public ILoggingEvent fromGenericRecord(GenericRecord datum) {
    return LoggingEvent.decode(datum);
  }
}
