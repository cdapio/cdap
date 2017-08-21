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

package co.cask.cdap.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.logging.LoggingUtil;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
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

  /**
   * Encodes a {@link ILoggingEvent} to byte array.
   */
  public byte[] toBytes(ILoggingEvent event) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(getAvroSchema());
    try {
      writer.write(toGenericRecord(event), encoder);
    } catch (IOException e) {
      // This shouldn't happen since we are writing to byte array output stream.
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
    return new LoggingEvent(toGenericRecord(buffer), buffer);
  }

  /**
   * Decodes the timestamp of a {@link ILoggingEvent} encoded in the given {@link ByteBuffer} with the schema
   * returned by the {@link #getAvroSchema()} method.
   *
   * @param buffer the buffer to decode
   * @return the event timestamp
   * @throws IOException if fail to decode
   */
  public long decodeEventTimestamp(ByteBuffer buffer) throws IOException {
    BinaryDecoder decoder = getDecoder(buffer);

    for (Schema.Field field : getAvroSchema().getFields()) {
      if ("timestamp".equals(field.name())) {
        return decoder.readLong();
      } else {
        // This shouldn't be called as the "timestamp" is the first in the schema
        // However we include it in here for future comparability.
        skip(field.schema(), decoder);
      }
    }

    // If reached here, meaning the timestamp is not found, which shouldn't happen.
    throw new IOException("Missing timestamp field in the LoggingEvent schema");
  }

  private BinaryDecoder getDecoder(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      decoder = DecoderFactory.get().binaryDecoder(buffer.array(), buffer.arrayOffset() + buffer.position(),
                                                   buffer.remaining(), decoder);
    } else {
      decoder = DecoderFactory.get().binaryDecoder(Bytes.toBytes(buffer), decoder);
    }
    return decoder;
  }

  /**
   * Decodes the content of the given {@link ByteBuffer} into {@link GenericRecord}, based on the schema
   * returned by the {@link #getAvroSchema()} method.
   *
   * @param buffer the buffer to decode
   * @return a {@link GenericRecord} representing the decoded content.
   * @throws IOException if fail to decode
   */
  private GenericRecord toGenericRecord(ByteBuffer buffer) throws IOException {
    return datumReader.read(null, getDecoder(buffer));
  }

  /**
   * Creates a new {@link GenericRecord} that represents the given {@link ILoggingEvent}.
   */
  public GenericRecord toGenericRecord(ILoggingEvent event) {
    Schema schema = getAvroSchema();
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("threadName", event.getThreadName());
    datum.put("level", event.getLevel() == null ? Level.ERROR_INT : event.getLevel().toInt());
    datum.put("message", event.getMessage());

    Object[] arguments = event.getArgumentArray();
    if (arguments != null) {
      GenericArray<String> argArray =
        new GenericData.Array<>(arguments.length,
                                schema.getField("argumentArray").schema().getTypes().get(1));
      for (Object argument : arguments) {
        argArray.add(argument == null ? null : argument.toString());
      }
      datum.put("argumentArray", argArray);
    }

    datum.put("formattedMessage", event.getFormattedMessage());
    datum.put("loggerName", event.getLoggerName());
    datum.put("loggerContextVO", LoggerContextSerializer.encode(schema.getField("loggerContextVO").schema(),
                                                                event.getLoggerContextVO()));
    datum.put("throwableProxy", ThrowableProxySerializer.encode(schema.getField("throwableProxy").schema(),
                                                                event.getThrowableProxy()));
    if (event.hasCallerData()) {
      datum.put("callerData", CallerDataSerializer.encode(schema.getField("callerData").schema(),
                                                          event.getCallerData()));
    }
    datum.put("hasCallerData", event.hasCallerData());
    //datum.put("marker", marker);
    datum.put("mdc", LoggingUtil.encodeMDC(event.getMDCPropertyMap()));
    datum.put("timestamp", event.getTimeStamp());
    return datum;
  }

  /**
   * Skips data from the decoder based on the schema.
   */
  private void skip(Schema schema, Decoder decoder) throws IOException {
    switch (schema.getType()) {
      case RECORD:
        for (Schema.Field f : schema.getFields()) {
          skip(f.schema(), decoder);
        }
        break;
      case ENUM:
        decoder.readEnum();
        break;
      case ARRAY:
        for (long i = decoder.skipArray(); i != 0; i = decoder.skipArray()) {
          for (long j = 0; j < i; j++) {
            skip(schema.getElementType(), decoder);
          }
        }
        break;
      case MAP:
        for (long i = decoder.skipMap(); i != 0; i = decoder.skipMap()) {
          for (long j = 0; j < i; j++) {
            decoder.skipString();  // Discard key
            skip(schema.getValueType(), decoder);
          }
        }
        break;
      case UNION:
        decoder.readIndex();
        break;
      case FIXED:
        decoder.skipFixed(schema.getFixedSize());
        break;
      case STRING:
        decoder.skipString();
        break;
      case BYTES:
        decoder.skipBytes();
        break;
      case INT:
        decoder.readInt();
        break;
      case LONG:
        decoder.readLong();
        break;
      case FLOAT:
        decoder.readFloat();
        break;
      case DOUBLE:
        decoder.readDouble();
        break;
      case BOOLEAN:
        decoder.readBoolean();
        break;
      case NULL:
        decoder.readNull();
        break;
    }
  }
}
