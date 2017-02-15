/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.serialize.LoggingEvent;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Avro serializer for ILoggingEvent.
 * Method of this class is not thread safe, hence cannot be called from multiple threads concurrently.
 */
@NotThreadSafe
public final class LoggingEventSerializer {

  private final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(getAvroSchema());
  private BinaryDecoder decoder;

  /**
   * Returns the {@link Schema} for logging event, which is the same as {@link LogSchema.LoggingEvent#SCHEMA}.
   */
  public Schema getAvroSchema() {
    return LogSchema.LoggingEvent.SCHEMA;
  }

  public byte[] toBytes(ILoggingEvent loggingEvent) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(getAvroSchema());
    try {
      writer.write(LoggingEvent.encode(getAvroSchema(), loggingEvent), encoder);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return out.toByteArray();
  }

  /**
   * Decodes the content of the given {@link ByteBuffer} into {@link ILoggingEvent}, based on the
   * schema returned by the {@link #getAvroSchema()} method.
   *
   * @param buffer the buffer to decode
   * @return a new instance of {@link ILoggingEvent} decoded from the buffer
   * @throws IOException if fail to decode
   */
  public ILoggingEvent fromBytes(ByteBuffer buffer) throws IOException {
    return LoggingEvent.decode(toGenericRecord(buffer));
  }

  /**
   * Decodes the content of the given {@link ByteBuffer} into {@link GenericRecord}, based on the schema
   * returned by the {@link #getAvroSchema()} method.
   *
   * @param buffer the buffer to decode
   * @return a {@link GenericRecord} representing the decoded content.
   * @throws IOException if fail to decode
   */
  public GenericRecord toGenericRecord(ByteBuffer buffer) throws IOException {
    if (buffer.hasArray()) {
      decoder = DecoderFactory.get().binaryDecoder(buffer.array(), buffer.arrayOffset() + buffer.position(),
                                                   buffer.remaining(), decoder);
    } else {
      decoder = DecoderFactory.get().binaryDecoder(Bytes.toBytes(buffer), decoder);
    }
    return datumReader.read(null, decoder);
  }
}
