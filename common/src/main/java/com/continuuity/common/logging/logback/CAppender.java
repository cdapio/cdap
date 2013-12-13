/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.common.HumanReadableLogMessageFormat;
import com.continuuity.common.logging.common.LogMessageFormat;
import com.continuuity.common.logging.common.LogWriter;

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
