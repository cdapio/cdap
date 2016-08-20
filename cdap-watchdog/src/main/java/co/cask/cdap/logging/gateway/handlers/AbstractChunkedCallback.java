/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LogReader Callback class that uses {@link ChunkResponder} to send logs back in chunks.
 */
public abstract class AbstractChunkedCallback implements Callback {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractChunkedCallback.class);

  private final AtomicBoolean initialized = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final HttpResponder responder;
  private final ByteBuffer chunkBuffer = ByteBuffer.allocate(8 * 1024);
  private final CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
  private final AtomicInteger count = new AtomicInteger();
  private ChunkResponder chunkResponder;

  AbstractChunkedCallback(HttpResponder responder) {
    this.responder = responder;
  }

  @Override
  public void init() {
    // if initialized already, then return
    if (!initialized.compareAndSet(false, true)) {
      return;
    }
    chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK, getResponseHeaders());
  }

  @Override
  public int getCount() {
    return count.get();
  }

  /**
   * Return {@link Multimap} of HTTP response headers
   */
  protected abstract Multimap<String, String> getResponseHeaders();

  /**
   * Handle each {@link LogEvent}
   */
  protected abstract void handleEvent(LogEvent event);

  protected void writeFinal() throws IOException {
    encodeSend(CharBuffer.allocate(0), true);
  }

  @Override
  public final void handle(LogEvent event) {
    handleEvent(event);
    count.incrementAndGet();
  }

  @Override
  public void close() {
    // If closed already, then return
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      // Write the last chunk
      writeFinal();
      // Flush the encoder
      CoderResult coderResult;
      do {
        coderResult = charsetEncoder.flush(chunkBuffer);
        chunkBuffer.flip();
        chunkResponder.sendChunk(ChannelBuffers.copiedBuffer(chunkBuffer));
        chunkBuffer.clear();
      } while (coderResult.isOverflow());
    } catch (IOException e) {
      // If cannot send chunks, nothing can be done (since the client closed connection).
      // Just log the error as debug.
      LOG.debug("Failed to send chunk", e);
    } finally {
      Closeables.closeQuietly(chunkResponder);
    }
  }

  protected void encodeSend(CharBuffer inBuffer, boolean endOfInput) throws IOException {
    while (true) {
      CoderResult coderResult = charsetEncoder.encode(inBuffer, chunkBuffer, endOfInput);
      if (coderResult.isOverflow()) {
        // if reached buffer capacity then flush chunk
        chunkBuffer.flip();
        chunkResponder.sendChunk(ChannelBuffers.copiedBuffer(chunkBuffer));
        chunkBuffer.clear();
      } else if (coderResult.isError()) {
        // skip characters causing error, and retry
        inBuffer.position(inBuffer.position() + coderResult.length());
      } else {
        // log line was completely written
        break;
      }
    }
  }
}
