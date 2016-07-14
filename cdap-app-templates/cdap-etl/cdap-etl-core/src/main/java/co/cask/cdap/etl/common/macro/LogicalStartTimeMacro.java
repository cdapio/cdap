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

package co.cask.cdap.etl.common.macro;


import com.google.common.annotations.VisibleForTesting;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * LogicalStartTimeMacro macros use the logical start time of a run to perform substitution.
 * LogicalStartTimeMacro macros follow the syntax ${logicalStartTime(arguments)}. Arguments are expected to be either:
 *
 * empty string
 * format
 * format,offset
 * format,offset,timezone
 *
 * If no format is given, the runtime in milliseconds will be used.
 * Otherwise, the format is expected to be a SimpleDateFormat that will be used to format the runtime.
 * The offset can be used to specify some amount of time to subtract from the runtime before formatting it.
 * The offset must be parse-able by {@link TimeParser}, which allows some simple math expressions.
 * For example, suppose the logical start time of the run is 2016-01-01T00:00:00 and we have macro
 * ${logicalStartTime(yyyy-MM-dd'T'HH-mm-ss,1d-4h)}. The format is yyyy-MM-dd'T'HH-mm-ss and the offset is 1d-4h+30m.
 * This means the macro will be replaced with 2015-12-31T03:30:00, since the offset translates to 20.5 hours, so
 * the whole macro evaluates to 20.5 hours before midnight of new years 2016.
 */
public class LogicalStartTimeMacro {
  private final TimeZone defaultTimeZone;

  public LogicalStartTimeMacro() {
    this(TimeZone.getDefault());
  }

  @VisibleForTesting
  LogicalStartTimeMacro(TimeZone defaultTimeZone) {
    this.defaultTimeZone = defaultTimeZone;
  }

  public String evaluate(long logicalStartTime, String... arguments) {
    if (arguments.length == 1 && arguments[0].isEmpty()) {
      return String.valueOf(logicalStartTime);
    }

    SimpleDateFormat dateFormat;
    long offset = 0;
    TimeZone timeZone = defaultTimeZone;

    if (arguments.length > 3) {
      throw new IllegalArgumentException("runtime macro supports at most 3 arguments - format, offset, and timezone. " +
                                           "Formats containing a comma are not supported.");
    }

    dateFormat = new SimpleDateFormat(arguments[0]);
    if (arguments.length > 1) {
      TimeParser timeParser = new TimeParser(logicalStartTime);
      offset = timeParser.parseRuntime(arguments[1].trim());
      if (arguments.length > 2) {
        timeZone = TimeZone.getTimeZone(arguments[2].trim());
      }
    }
    dateFormat.setTimeZone(timeZone);

    Date date = new Date(logicalStartTime - offset);
    return dateFormat.format(date);
  }
}
