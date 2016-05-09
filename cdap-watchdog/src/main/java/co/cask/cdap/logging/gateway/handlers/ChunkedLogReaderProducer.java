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
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.BodyProducer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Closeables;
import org.apache.commons.lang.StringEscapeUtils;
import org.jboss.netty.buffer.ChannelBuffer;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LogReader callback to encode log events, and send them as chunked stream.
 */
class ChunkedLogReaderProducer extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(ChunkedLogReaderProducer.class);

  private final CloseableIterator<LogEvent> logEventIter;

  private final ByteBuffer chunkBuffer = ByteBuffer.allocate(8 * 1024);
  private final CharsetEncoder charsetEncoder = Charset.forName("UTF-8").newEncoder();
  private final PatternLayout patternLayout;
  private final boolean escape;
  private final AtomicInteger count = new AtomicInteger();

  private boolean doneSending = false;


  ChunkedLogReaderProducer(CloseableIterator<LogEvent> logEventIter, String logPattern, boolean escape) {
    this.logEventIter = logEventIter;

    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    this.patternLayout = new PatternLayout();
    this.patternLayout.setContext(loggerContext);
    this.patternLayout.setPattern(logPattern);

    patternLayout.start();
  }

  private void close() {
    try {
      patternLayout.stop();
    } finally {
      logEventIter.close();
    }
  }

  @Override
  public ChannelBuffer nextChunk() throws Exception {
    if (!logEventIter.hasNext()) {
      if (doneSending) {
        return ChannelBuffers.EMPTY_BUFFER;
      }

      // Write the last chunk
      chunkBuffer.put((byte) 0);

      // Flush the encoder
      CoderResult coderResult = charsetEncoder.flush(chunkBuffer);
      chunkBuffer.flip();
      ChannelBuffer toReturn = ChannelBuffers.copiedBuffer(chunkBuffer);

      chunkBuffer.clear();
      if (!coderResult.isOverflow()) {
        doneSending = true;
      }
      return toReturn;

    }

    LogEvent event = logEventIter.next();
    String logLine = patternLayout.doLayout(event.getLoggingEvent());
    logLine = escape ? StringEscapeUtils.escapeHtml(logLine) : logLine;
/*
    try {
      // Encode logLine and send chunks
      CharBuffer inBuffer = CharBuffer.wrap(logLine);
      while (true) {
        CoderResult coderResult = charsetEncoder.encode(inBuffer, chunkBuffer, false);
        if (coderResult.isOverflow()) {
          // if reached buffer capacity then flush chunk
          chunkBuffer.flip();
          ChannelBuffer toReturn = ChannelBuffers.copiedBuffer(chunkBuffer);
          chunkBuffer.clear();
          return null;
        } else if (coderResult.isError()) {
          // skip characters causing error, and retry
          inBuffer.position(inBuffer.position() + coderResult.length());
        } else {
          // log line was completely written
          break;
        }
      }



      count.incrementAndGet();
    } catch (IOException e) {
      // Just propagate the exception, the caller of this Callback should be handling it.
      throw Throwables.propagate(e);
    }
    */
    return null;
  }

  @Override
  public void finished() throws Exception {
    close();
  }

  @Override
  public void handleError(Throwable cause) {
    close();
  }
}
