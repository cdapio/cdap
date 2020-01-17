/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.format;

import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import org.apache.avro.SchemaParseException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * A {@link RecordFormat} that interprets the input as avro encoded binary data.
 */
public class AvroRecordFormat extends RecordFormat<ByteBuffer, StructuredRecord> {
  private final ByteBufferInputStream byteBufferInput = new ByteBufferInputStream(ByteBuffer.wrap(new byte[0]));
  private final DecoderFactory decoderFactory = DecoderFactory.get();

  private Schema formatSchema;
  private org.apache.avro.Schema avroFormatSchema;

  private StructuredRecordDatumReader datumReader;
  private BinaryDecoder binaryDecoder;

  @Override
  public StructuredRecord read(ByteBuffer event) {
    try {
      binaryDecoder = decoderFactory.binaryDecoder(byteBufferInput.reset(event), binaryDecoder);
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
    datumReader = new StructuredRecordDatumReader(formatSchema, avroFormatSchema);
    datumReader.setSchema(avroFormatSchema);
  }

  /**
   * {@link InputStream} that reads a {@link ByteBuffer}.
   */
  @NotThreadSafe
  public static final class ByteBufferInputStream extends InputStream {

    private ByteBuffer buffer;

    ByteBufferInputStream(ByteBuffer buffer) {
      reset(buffer);
    }

    public ByteBufferInputStream reset(ByteBuffer buffer) {
      this.buffer = buffer;
      return this;
    }

    @Override
    public int read() {
      if (buffer.remaining() <= 0) {
        return -1;
      }
      return buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
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
