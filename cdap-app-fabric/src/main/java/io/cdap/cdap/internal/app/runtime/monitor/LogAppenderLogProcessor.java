/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.inject.Inject;

/**
 * Log processor which writes the logging event to a {@link LogAppender}
 */
public class LogAppenderLogProcessor implements RemoteExecutionLogProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LogAppenderLogProcessor.class);
  private static final ThreadLocal<LoggingEventSerializer> LOGGING_EVENT_SERIALIZER =
          ThreadLocal.withInitial(LoggingEventSerializer::new);
  private final LogAppender logAppender;

  @Inject
  LogAppenderLogProcessor(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  @Override
  public void process(Iterator<byte[]> loggingEventBytes) {

    LoggingEventSerializer serializer = LOGGING_EVENT_SERIALIZER.get();
    loggingEventBytes.forEachRemaining(bytes -> {
      try {
        ILoggingEvent iLoggingEvent = serializer.fromBytes(ByteBuffer.wrap(bytes));
        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(iLoggingEvent.getMDCPropertyMap());
        if (loggingContext == null) {
          // This shouldn't happen
          LOG.debug("Ignore logging event due to missing logging context: {}", iLoggingEvent);
          return;
        }
        logAppender.append(new LogMessage(iLoggingEvent, loggingContext));
      } catch (IOException e) {
        LOG.warn("Ignore logging event due to decode failure: {}", e.getMessage());
        LOG.debug("Ignore logging event stack trace", e);
      }
    });
  }
}
