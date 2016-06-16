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
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringEscapeUtils;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * LogReader callback to encode log events, as text.
 */
class TextCallback extends AbstractChunkedCallback {
  private final PatternLayout patternLayout;
  private final boolean escape;

  TextCallback(HttpResponder responder, String logPattern, boolean escape) {
    super(responder);
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
    super.init();
    patternLayout.start();
  }

  @Override
  public Multimap<String, String> getResponseHeaders() {
    return ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=utf-8");
  }

  @Override
  public void handleEvent(LogEvent event) {
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
    super.close();
    patternLayout.stop();
  }
}
