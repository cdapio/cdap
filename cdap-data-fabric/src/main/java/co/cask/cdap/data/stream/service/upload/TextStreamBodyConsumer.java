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

import co.cask.cdap.proto.id.StreamId;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link BodyConsumer} for consuming line separated text file upload.
 */
@NotThreadSafe
final class TextStreamBodyConsumer extends BodyConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(TextStreamBodyConsumer.class);

  private final StreamId streamId;
  private final ContentWriterFactory contentWriterFactory;
  private ContentWriter contentWriter;
  private boolean failed;
  private ByteBuf buffer = Unpooled.EMPTY_BUFFER;

  TextStreamBodyConsumer(ContentWriterFactory contentWriterFactory) {
    this.streamId = contentWriterFactory.getStream();
    this.contentWriterFactory = contentWriterFactory;
  }

  @Override
  public void chunk(ByteBuf chunk, HttpResponder responder) {
    if (failed) {
      return;
    }

    ByteBuf contentChunk = chunk;
    if (buffer.isReadable()) {
      contentChunk = Unpooled.wrappedBuffer(buffer, contentChunk);
      buffer = Unpooled.EMPTY_BUFFER;
    }

    try {
      processChunk(contentChunk);
      if (contentChunk.isReadable()) {
        buffer = contentChunk.copy();
      }
    } catch (Exception e) {
      failed = true;
      LOG.error("Failed to write upload content to stream {}", streamId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to write uploaded content");

      // Propagate the exception so that the netty http service will terminate the handling
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void finished(HttpResponder responder) {
    try {
      // Process any leftover content.
      ContentWriter writer = getContentWriter();
      if (buffer.isReadable()) {
        processChunk(buffer);

        if (buffer.isReadable()) {
          writer.append(buffer.nioBuffer(), false);
        }
      }
      writer.close();
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      // Just log and response. No need to propagate since it's the end of upload already.
      LOG.error("Failed to write upload content to stream {}", streamId, e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to write uploaded content");
    }
  }

  @Override
  public void handleError(Throwable cause) {
    // Nothing other than log
    LOG.warn("Failed to handle upload to stream {}", streamId, cause);
  }

  private void processChunk(final ByteBuf chunk) throws IOException {
    final ContentWriter writer = getContentWriter();
    writer.appendAll(new AbstractIterator<ByteBuffer>() {
      @Override
      protected ByteBuffer computeNext() {
        int len = chunk.bytesBefore((byte) '\n');
        if (len < 0) {
          return endOfData();
        }
        ByteBuf body;
        if (len > 0) {
          body = chunk.readSlice(len);
          if (body.getByte(body.writerIndex() - 1) == (byte) '\r') {
            body.writerIndex(body.writerIndex() - 1);
          }
        } else {
          body = Unpooled.EMPTY_BUFFER;
        }

        // Swallow the '\n'
        chunk.readByte();
        return body.nioBuffer();
      }
    }, false);
  }

  private ContentWriter getContentWriter() throws IOException {
    if (contentWriter == null) {
      contentWriter = contentWriterFactory.create(ImmutableMap.<String, String>of());
    }
    return contentWriter;
  }
}
