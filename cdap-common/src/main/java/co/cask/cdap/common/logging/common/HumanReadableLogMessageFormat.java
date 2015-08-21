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

package co.cask.cdap.common.logging.common;

import co.cask.cdap.common.logging.LoggingContext;

/**
 * A message format that is human readable.
 */
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

    if (traceLines != null) {
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
