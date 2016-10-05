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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.logging.read.LogEvent;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringEscapeUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * LogReader BodyProducer to encode log events, as text.
 */
class TextChunkedLogProducer extends AbstractChunkedLogProducer {
  private final PatternLayout patternLayout;
  private final boolean escape;
  private final ChannelBuffer channelBuffer;

  TextChunkedLogProducer(CloseableIterator<LogEvent> logEventIter, String logPattern, boolean escape) {
    super(logEventIter);
    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    patternLayout = new PatternLayout();
    patternLayout.setContext(loggerContext);
    patternLayout.setPattern(logPattern);
    patternLayout.start();

    channelBuffer = ChannelBuffers.dynamicBuffer(BUFFER_BYTES);
  }

  @Override
  public Multimap<String, String> getResponseHeaders() {
    return ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
  }

  @Override
  protected ChannelBuffer writeLogEvents(CloseableIterator<LogEvent> logEventIter) throws IOException {
    channelBuffer.clear();
    while (logEventIter.hasNext() && channelBuffer.readableBytes() < BUFFER_BYTES) {
      LogEvent logEvent = logEventIter.next();
      String logLine = patternLayout.doLayout(logEvent.getLoggingEvent());
      logLine = escape ? StringEscapeUtils.escapeHtml(logLine) : logLine;
      channelBuffer.writeBytes(Bytes.toBytes(logLine));
    }
    return channelBuffer;
  }

  @Override
  protected ChannelBuffer onWriteStart() throws IOException {
    return ChannelBuffers.EMPTY_BUFFER;
  }

  @Override
  protected ChannelBuffer onWriteFinish() throws IOException {
    return ChannelBuffers.EMPTY_BUFFER;
  }

  @Override
  public void close() {
    try {
      patternLayout.stop();
    } finally {
      super.close();
    }
  }
}
