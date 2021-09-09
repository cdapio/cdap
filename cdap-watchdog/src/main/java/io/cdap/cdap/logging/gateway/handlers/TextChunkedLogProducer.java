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

package io.cdap.cdap.logging.gateway.handlers;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.logging.read.LogEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * LogReader BodyProducer to encode log events, as text.
 */
public class TextChunkedLogProducer extends AbstractChunkedLogProducer {
  private final PatternLayout patternLayout;
  private final boolean escape;

  public TextChunkedLogProducer(CloseableIterator<LogEvent> logEventIter, String logPattern, boolean escape) {
    super(logEventIter);
    this.escape = escape;

    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();

    patternLayout = new PatternLayout();
    patternLayout.setContext(loggerContext);
    patternLayout.setPattern(logPattern);
    patternLayout.start();
  }

  @Override
  public HttpHeaders getResponseHeaders() {
    return new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
  }

  @Override
  protected ByteBuf writeLogEvents(CloseableIterator<LogEvent> logEventIter) throws IOException {
    ByteBuf buffer = Unpooled.buffer(BUFFER_BYTES);

    while (logEventIter.hasNext() && buffer.readableBytes() < BUFFER_BYTES) {
      LogEvent logEvent = logEventIter.next();
      String logLine = patternLayout.doLayout(logEvent.getLoggingEvent());
      logLine = escape ? StringEscapeUtils.escapeHtml(logLine) : logLine;
      buffer.writeCharSequence(logLine, StandardCharsets.UTF_8);
    }
    return buffer;
  }

  @Override
  protected ByteBuf onWriteStart() throws IOException {
    return Unpooled.EMPTY_BUFFER;
  }

  @Override
  protected ByteBuf onWriteFinish() throws IOException {
    return Unpooled.EMPTY_BUFFER;
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
