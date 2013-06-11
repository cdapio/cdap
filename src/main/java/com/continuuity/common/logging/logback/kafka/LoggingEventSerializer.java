package com.continuuity.common.logging.logback.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.logback.serialize.LoggingEvent;
import com.google.common.base.Throwables;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
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

/**
 * Avro serializer for ILoggingEvent.
 */
@SuppressWarnings("UnusedDeclaration")
public class LoggingEventSerializer implements Encoder<ILoggingEvent>, Decoder<ILoggingEvent> {
  private final Schema schema;

  public LoggingEventSerializer() throws IOException {
    this.schema = new Schema.Parser().parse(getClass().getResourceAsStream("/logging/schema/LoggingEvent.avsc"));
  }

  public LoggingEventSerializer(VerifiableProperties props) throws IOException {
    this();
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public byte[] toBytes(ILoggingEvent loggingEvent) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    try {
      writer.write(LoggingEvent.encode(schema, loggingEvent), encoder);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return out.toByteArray();
  }

  @Override
  public ILoggingEvent fromBytes(byte[] bytes) {
    return LoggingEvent.decode(toGenericRecord(bytes));
  }

  public GenericRecord toGenericRecord(byte[] bytes) {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
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
