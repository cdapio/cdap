/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.common;

import com.continuuity.common.logging.LoggingContext;

public class HumanReadableLogMessageFormat implements LogMessageFormat {
  /**
   * End of line, for our own internal presentation.
   *
   * <p>We use this symbol in order to separate lines in buffer, not in order
   * to show them to the user. Thus, it's platform independent symbol and
   * will work on any OS (incl. Windows).
   */
  public static final String EOL = "\n";

  /**
   * Prefix to be appended in the beginning of every trace line.
   *
   * <p>
   *   We use it to prefix trace lines so that later we can parse multi-line log messages
   * </p>
   */
  public static final String TRACE_LINE_PREFIX = "  ";

  @Override
  public String format(final String message, final String[] traceLines,
                       final LoggingContext context, final String[] userTags) {

    // Build the message to be logged and add it to queue.
    final StringBuilder sb = new StringBuilder();

    sb.append(contextToString(context)).append(" ");

    sb.append(message);

    if(traceLines != null) {
      for (String traceLine : traceLines) {
        sb.append(TRACE_LINE_PREFIX).append(traceLine).append(EOL);
      }
    }

    return sb.toString();
  }

  private String contextToString(final LoggingContext context) {
    // For now we don't need to inject context info in log message (with old logging back-end)
    return "";
  }
}
