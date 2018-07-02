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

package co.cask.cdap.logging.remote;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.base.Preconditions;
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
    LOG.info("log appender isStarted: ... {}", logAppender.isStarted());
    loggingEventBytes.forEachRemaining(bytes -> {
      try {
        ILoggingEvent iLoggingEvent =
                LOGGING_EVENT_SERIALIZER.get().fromBytes(ByteBuffer.wrap(bytes));
        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(iLoggingEvent.getMDCPropertyMap());
        logAppender.append(new LogMessage(iLoggingEvent, Preconditions.checkNotNull(loggingContext)));
      } catch (IOException e) {
        LOG.warn("Ignore logging event due to decode failure: {}", e.getMessage());
        LOG.debug("Ignore logging event stack trace", e);
      }
    });
  }
}
