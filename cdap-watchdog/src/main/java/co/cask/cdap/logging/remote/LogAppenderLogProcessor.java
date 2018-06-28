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
import com.google.common.base.Preconditions;

import javax.inject.Inject;

/**
 * Log processor which writes the logging event to a {@link LogAppender}
 */
public class LogAppenderLogProcessor implements RemoteExecutionLogProcessor {

  private final LogAppender logAppender;

  @Inject
  LogAppenderLogProcessor(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  @Override
  public void process(ILoggingEvent loggingEvent) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(loggingEvent.getMDCPropertyMap());
    logAppender.append(new LogMessage(loggingEvent, Preconditions.checkNotNull(loggingContext)));
  }
}
