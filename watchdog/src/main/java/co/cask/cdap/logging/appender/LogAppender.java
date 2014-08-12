/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;

/**
 * Continuuity log appender interface.
 */
public abstract class LogAppender extends AppenderBase<ILoggingEvent> {
  public final void append(ILoggingEvent eventObject) {
    LoggingContext loggingContext;
    // If the context is not setup, pickup the context from thread-local.
    // If the context is already setup, use the context (in async mode).
    if (eventObject instanceof LogMessage) {
      loggingContext = ((LogMessage) eventObject).getLoggingContext();
    } else {
      loggingContext = LoggingContextAccessor.getLoggingContext();
      if (loggingContext == null) {
        return;
      }
    }

    append(new LogMessage(eventObject, loggingContext));
  }

  protected abstract void append(LogMessage logMessage);

}
