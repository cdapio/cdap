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
import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bhooshan on 5/6/16.
 */
class MyLogReaderCallback implements Callback {

  private final HttpResponder responder;
  private final List<LogEvent> logResults = new ArrayList<>();
  private final AtomicInteger count = new AtomicInteger();
  private final PatternLayout patternLayout;

  MyLogReaderCallback(HttpResponder responder) {
    this.responder = responder;
    ch.qos.logback.classic.Logger rootLogger =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    LoggerContext loggerContext = rootLogger.getLoggerContext();
    this.patternLayout = new PatternLayout();
    this.patternLayout.setContext(loggerContext);
  }

  @Override
  public void init() {
    patternLayout.start();
  }

  @Override
  public void handle(LogEvent event) {
    logResults.add(event);
    count.incrementAndGet();
  }

  @Override
  public int getCount() {
    return count.get();
  }

  @Override
  public void close() {
    responder.sendJson(HttpResponseStatus.OK, logResults);
    patternLayout.stop();
  }
}
