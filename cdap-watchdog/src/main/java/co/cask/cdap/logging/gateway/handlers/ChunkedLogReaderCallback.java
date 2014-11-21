/*
 * Copyright © 2014 Cask Data, Inc.
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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Closeables;
import org.apache.commons.lang.StringEscapeUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

/**
 * LogReader callback to encode log events, and send them as chunked stream.
 */
class ChunkedLogReaderCallback implements Callback {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkedLogReaderCallback.class);

  private final ByteBuffer chunkBuffer = ByteBuffer.allocate(8 * 1024);
  private final CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
  private final HttpResponder responder;
  private final PatternLayout patternLayout;
  private final boolean escape;
  private ChunkResponder chunkResponder;

  ChunkedLogReaderCallback(HttpResponder responder, String logPattern, boolean escape) {
    this.responder = responder;
    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    this.patternLayout = new PatternLayout();
    this.patternLayout.setContext(loggerContext);
    this.patternLayout.setPattern(logPattern);
  }

  @Override
  public void init() {
    patternLayout.start();
    chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                                   "text/plain; charset=utf-8"));
  }

  @Override
  public void handle(LogEvent event) {
    String logLine = patternLayout.doLayout(event.getLoggingEvent());
    logLine = escape ? StringEscapeUtils.escapeHtml(logLine) : logLine;

    try {
      // Encode logLine and send chunks
      encodeSend(CharBuffer.wrap(logLine), false);
    } catch (IOException e) {
      // Just propagate the exception, the caller of this Callback should be handling it.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    try {
      // Write the last chunk
      encodeSend(CharBuffer.allocate(0), true);
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
      try {
        patternLayout.stop();
      } finally {
        Closeables.closeQuietly(chunkResponder);
      }
    }
  }

  private void encodeSend(CharBuffer inBuffer, boolean endOfInput) throws IOException {
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
