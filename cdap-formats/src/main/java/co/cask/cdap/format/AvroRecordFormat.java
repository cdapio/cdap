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

package co.cask.cdap.format;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.spi.stream.AbstractStreamEventRecordFormat;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.SchemaParseException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Stream record format that interprets the body as avro encoded binary data.
 */
public class AvroRecordFormat extends AbstractStreamEventRecordFormat<StructuredRecord> {
  private final ByteBufferInputStream byteBufferInput = new ByteBufferInputStream(ByteBuffer.wrap(new byte[0]));
  private final DecoderFactory decoderFactory = DecoderFactory.get();

  @VisibleForTesting
  static final String SCHEMA = "schema";
  @VisibleForTesting
  static final String SCHEMA_HASH = "schema.hash";

  private Schema formatSchema;
  private org.apache.avro.Schema avroFormatSchema;

  private String formatSchemaHash;
  private String eventSchemaHash;
  private StructuredRecordDatumReader datumReader;
  private BinaryDecoder binaryDecoder;

  @Override
  public StructuredRecord read(StreamEvent event) {
    try {
      // Check if the event has different schema then the read schema. If it does update the datumReader
      String eventSchemaStr = event.getHeaders().get(SCHEMA);
      if (eventSchemaStr != null) {
        String eventSchemaHash = event.getHeaders().get(SCHEMA_HASH);
        if (!this.eventSchemaHash.equals(eventSchemaHash)) {
          org.apache.avro.Schema eventSchema = new org.apache.avro.Schema.Parser().parse(eventSchemaStr);
          datumReader.setSchema(eventSchema);
          this.eventSchemaHash = eventSchemaHash;
        }
      } else {
        // If no schema is available on the event, assume it's the same as read schema
        datumReader.setSchema(avroFormatSchema);
        eventSchemaHash = formatSchemaHash;
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
      formatSchemaHash = Bytes.toHexString(md5.digest(Bytes.toBytes(avroFormatSchema.toString())));
      eventSchemaHash = formatSchemaHash;
      datumReader = new StructuredRecordDatumReader(formatSchema, avroFormatSchema);
    } catch (NoSuchAlgorithmException e) {
      // This shouldn't happen.
      throw new RuntimeException(e);
    }
  }

  /**
   * {@link InputStream} that reads a {@link ByteBuffer}.
   */
  @NotThreadSafe
  private static final class ByteBufferInputStream extends InputStream {

    private ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
      reset(buffer);
    }

    public ByteBufferInputStream reset(ByteBuffer buffer) {
      this.buffer = buffer;
      return this;
    }

    @Override
    public int read() throws IOException {
      if (buffer.remaining() <= 0) {
        return -1;
      }
      return buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int remaining = buffer.remaining();
      if (remaining <= 0) {
        return -1;
      }
      if (len <= remaining) {
        buffer.get(b, off, len);
        return len;
      } else {
        buffer.get(b, off, remaining);
        return remaining;
      }
    }

    @Override
    public long skip(long n) throws IOException {
      if (n > Integer.MAX_VALUE) {
        throw new IOException("Cannot skip more then " + n + " bytes.");
      }
      int skips = (int) n;
      if (skips > buffer.remaining()) {
        skips = buffer.remaining();
      }
      buffer.position(buffer.position() + skips);
      return skips;
    }
  }
}
