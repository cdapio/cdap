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

package co.cask.cdap.data.stream.service.upload;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.ByteBuffers;
import co.cask.common.io.ByteBufferInputStream;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link BodyConsumer} that process Avro object file and write to stream.
 */
@NotThreadSafe
final class AvroStreamBodyConsumer extends BodyConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(AvroStreamBodyConsumer.class);

  private final AsyncChannelBufferInputStream bufferInput;
  private final ContentWriterThread writerThread;
  private final ContentWriterFactory contentWriterFactory;
  private boolean failed;

  AvroStreamBodyConsumer(ContentWriterFactory contentWriterFactory) {
    this.contentWriterFactory = contentWriterFactory;
    this.bufferInput = new AsyncChannelBufferInputStream();
    this.writerThread = new ContentWriterThread(bufferInput, contentWriterFactory);
  }

  @Override
  public void chunk(ChannelBuffer request, HttpResponder responder) {
    if (failed || (failed = respondIfFailed(responder))) {
      return;
    }
    if (!writerThread.isAlive()) {
      writerThread.start();
    }
    try {
      bufferInput.append(request);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void finished(HttpResponder responder) {
    try {
      // Signal the end of input and wait for the writer thread to complete
      bufferInput.append(ChannelBuffers.EMPTY_BUFFER);
      writerThread.join();

      if (!respondIfFailed(responder)) {
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (InterruptedException e) {
      // Just log and response. No need to propagate since it's the end of upload already.
      LOG.warn("Join on writer thread interrupted", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to complete upload due to interruption");
    } catch (IOException e) {
      // Just log and response. No need to propagate since it's the end of upload already.
      LOG.error("Failed to write upload content to stream {}", contentWriterFactory.getStream(), e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to write uploaded content");
    }
  }

  @Override
  public void handleError(Throwable cause) {
    LOG.warn("Failed to handle upload to stream {}", contentWriterFactory.getStream(), cause);
    Closeables.closeQuietly(bufferInput);
    try {
      writerThread.join();
    } catch (InterruptedException e) {
      // Just log
      LOG.warn("Join on writer thread interrupted", e);
    }
  }

  /**
   * Writes a failure status if the writer thread is failed.
   */
  private boolean respondIfFailed(HttpResponder responder) {
    Throwable failure = writerThread.getFailure();
    if (failure == null) {
      return false;
    }
    LOG.debug("Upload failed", failure);
    responder.sendString(HttpResponseStatus.BAD_REQUEST,
                         "Failed to process uploaded avro file: " + failure.getMessage());
    return true;
  }

  /**
   * The thread for reading Avro file and writing encoded Avro object to {@link ContentWriter}.
   */
  private static final class ContentWriterThread extends Thread {
    private static final AtomicLong id = new AtomicLong();

    private final InputStream contentStream;
    private final ContentWriterFactory contentWriterFactory;
    private volatile Throwable failure;

    public ContentWriterThread(InputStream contentStream, ContentWriterFactory contentWriterFactory) {
      super("avro-uploader-" + contentWriterFactory.getStream() + "-" + id.getAndIncrement());
      this.contentStream = contentStream;
      this.contentWriterFactory = contentWriterFactory;
    }

    @Override
    public void run() {
      try {
        // Create an Avro reader from the given content stream.
        DataFileStream<Object> reader = new DataFileStream<Object>(contentStream, new DummyDatumReader());
        try {
          Schema schema = reader.getSchema();
          DecoderFactory decoderFactory = DecoderFactory.get();
          ByteBufferInputStream blockInput = new ByteBufferInputStream(ByteBuffers.EMPTY_BUFFER);
          BinaryDecoder decoder = null;

          // Create the {@link ContentWriter} for writing encoded Avro content to stream
          String schemaStr = schema.toString();
          ContentWriter contentWriter = contentWriterFactory.create(ImmutableMap.of(
            Constants.Stream.Headers.SCHEMA, schemaStr,
            Constants.Stream.Headers.SCHEMA_HASH, Hashing.md5().hashString(schemaStr, Charsets.UTF_8).toString()
          ));

          try {
            while (reader.hasNext()) {
              // Instead of decoding the whole object, we just keep finding the object boundary in
              // an Avro raw data block.
              ByteBuffer block = reader.nextBlock();
              decoder = decoderFactory.directBinaryDecoder(blockInput.reset(block), decoder);
              int limit = block.limit();

              // Since we are depending on position in the block buffer, a direct binary decoder must be used
              // to ensure no buffering happens in the decoder.
              while (block.hasRemaining()) {
                // Mark the beginning of the datum
                block.mark();
                skipDatum(schema, decoder);

                // Mark the end of the datum
                int pos = block.position();
                block.reset();
                block.limit(pos);

                // Append the encoded datum to content writer.
                // Need to tell content write that the buffer provided is not immutable.
                contentWriter.append(block, false);

                // Restore the position and limit for the next datum
                block.position(pos);
                block.limit(limit);
              }
            }
            contentWriter.close();
          } catch (Throwable t) {
            contentWriter.cancel();
            throw t;
          }
        } finally {
          Closeables.closeQuietly(reader);
        }
      } catch (Throwable t) {
        failure = t;
      } finally {
        Closeables.closeQuietly(contentStream);
      }
    }

    public Throwable getFailure() {
      return failure;
    }

    /**
     * Skips a datum from the given decoder according to the given schema.
     */
    private void skipDatum(Schema schema, Decoder decoder) throws IOException {
      switch (schema.getType()) {
        case NULL:
          decoder.readNull();
          break;
        case BOOLEAN:
          decoder.readBoolean();
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
        case ENUM:
          decoder.readEnum();
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
        case RECORD:
          skipRecord(schema, decoder);
          break;
        case ARRAY:
          skipArray(schema, decoder);
          break;
        case MAP:
          skipMap(schema, decoder);
          break;
        case UNION:
          skipDatum(schema.getTypes().get(decoder.readIndex()), decoder);
          break;
      }
    }

    /**
     * Skips an array from the given decoder according to the given array schema.
     */
    private void skipArray(Schema schema, Decoder decoder) throws IOException {
      for (long count = decoder.skipArray(); count != 0; count = decoder.skipArray()) {
        while (count-- > 0) {
          skipDatum(schema.getElementType(), decoder);
        }
      }
    }

    /**
     * Skips a map from the given decoder according to the given map schema.
     */
    private void skipMap(Schema schema, Decoder decoder) throws IOException {
      for (long count = decoder.skipMap(); count != 0; count = decoder.skipMap()) {
        while (count-- > 0) {
          // Skip key
          decoder.skipString();
          skipDatum(schema.getValueType(), decoder);
        }
      }
    }

    /**
     * Skips a record from the given decoder according to the given record schema.
     */
    private void skipRecord(Schema schema, Decoder decoder) throws IOException {
      for (Schema.Field field : schema.getFields()) {
        skipDatum(field.schema(), decoder);
      }
    }
  }

  /**
   * A dummy {@link DatumReader} that reads nothing.
   */
  private static final class DummyDatumReader implements DatumReader<Object> {

    @Override
    public void setSchema(Schema schema) {
      // No-op
    }

    @Override
    public Object read(Object reuse, Decoder in) throws IOException {
      return null;
    }
  }
}
