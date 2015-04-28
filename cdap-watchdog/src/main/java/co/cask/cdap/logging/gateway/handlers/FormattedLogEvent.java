/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.logging.read.LogOffset;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
* Handles formatting of log event to send
*/
public final class FormattedLogEvent {
  private static final char SEPARATOR = '.';

  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String log;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String offset;

  public FormattedLogEvent(String log, LogOffset offset) {
    this.log = log;
    this.offset = formatLogOffset(offset);
  }

  public static String formatLogOffset(LogOffset logOffset) {
    return Joiner.on(SEPARATOR).join(logOffset.getKafkaOffset(), logOffset.getTime());
  }

  public static LogOffset parseLogOffset(String offsetStr) {
    if (offsetStr.isEmpty()) {
      return LogOffset.LATEST_OFFSET;
    }

    Iterable<String> splits = Splitter.on(SEPARATOR).split(offsetStr);
    Preconditions.checkArgument(Iterables.size(splits) == 2, "Invalid offset provided: %s", offsetStr);

    return new LogOffset(Long.valueOf(Iterables.get(splits, 0)), Long.valueOf(Iterables.get(splits, 1)));
  }
}
