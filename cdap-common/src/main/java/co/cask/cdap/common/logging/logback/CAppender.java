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

package co.cask.cdap.common.logging.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.common.HumanReadableLogMessageFormat;
import co.cask.cdap.common.logging.common.LogMessageFormat;
import co.cask.cdap.common.logging.common.LogWriter;

/**
 * Handles emitted log messages and delegates the actual writing to provided LogWriter.
 */
public class CAppender extends AppenderBase<ILoggingEvent> {
  // Hack hack hack: time constraints
  public static LogWriter logWriter = null;

  /**
   * Defines the format of the written log messages.
   */
  private LogMessageFormat format = new HumanReadableLogMessageFormat();

  /**
   * Layout handler.
   */
  private Layout<ILoggingEvent> layout;

  /**
   * @return Layout associated with logger.
   */
  public Layout<ILoggingEvent> getLayout() {
    return this.layout;
  }

  /**
   * Sets the layout.
   *
   * @param layout associated with logger.
   */
  public void setLayout(final Layout<ILoggingEvent> layout) {
    this.layout = layout;
  }

  @Override
  protected void append(final ILoggingEvent event) {
    // Hack hack hack: time constraints
    if (logWriter == null) {
      return;
    }

    LoggingContext context = LoggingContextAccessor.getLoggingContext();
    // Hack hack hack: time constraints
    if (context == null) {
      return;
    }

    String[] traceLines = null;
    if (event.getThrowableProxy() != null) {
      final StackTraceElementProxy[] exc
        = event.getThrowableProxy().getStackTraceElementProxyArray();
      if (exc != null) {
        traceLines = new String[exc.length];
        for (int i = 0; i < traceLines.length; i++) {
          traceLines[i] = exc[i].toString();
        }
      }
    }

    String msg = format.format(this.getLayout().doLayout(event),
                                         traceLines, context, null);

    logWriter.write(context.getLogPartition(), event.getLevel().toString(), msg);
  }
}
