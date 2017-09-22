/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.util;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing time strings into timestamps, with support for some basic time math.
 * Math syntax includes addition and subtraction in minutes, hours, and days.
 * The "now" keyword translates to the current epoch time in seconds, and can be strung together with
 * various additions and subtractions.  For example, "now" will translate into the current epoch time in seconds.
 * "now-5s" is 5 seconds before now, "now-1d" is one day before now, and "now-1d+4h" is 20 hours
 * before now.
 *
 */
public class TimeMathParser {

  private static final String NOW = "now";
  private static final String VALID_UNITS = "ms|s|m|h|d";
  private static final Pattern OP_PATTERN = Pattern.compile("([-+])(\\d+)(" + VALID_UNITS + ")");
  private static final Pattern RESOLUTION_PATTERN = Pattern.compile("(\\d+)(" + VALID_UNITS + ")");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^(\\d+)$");

  private static long convertToMilliseconds(String op, long num, String unitStr) {
    long milliseconds;
    if ("ms".equals(unitStr)) {
      milliseconds = num;
    } else if ("s".equals(unitStr)) {
      milliseconds = TimeUnit.SECONDS.toMillis(num);
    } else if ("m".equals(unitStr)) {
      milliseconds = TimeUnit.MINUTES.toMillis(num);
    } else if ("h".equals(unitStr)) {
      milliseconds = TimeUnit.HOURS.toMillis(num);
    } else if ("d".equals(unitStr)) {
      milliseconds = TimeUnit.DAYS.toMillis(num);
    } else {
      throw new IllegalArgumentException("invalid time unit " + unitStr +
                                           ", should be one of 'ms', 's', 'm', 'h', 'd'");
    }

    if ("+".equals(op)) {
      return milliseconds;
    } else if ("-".equals(op)) {
      return 0 - milliseconds;
    } else {
      throw new IllegalArgumentException("invalid operation " + op + ", should be either '+' or '-'");
    }
  }

  private static long convertToMilliseconds(long num, String unitStr) {
    return convertToMilliseconds("+", num, unitStr);
  }

  /**
   * @return the current time in seconds
   */
  @SuppressWarnings({"unused", "WeakerAccess"})
  public static long nowInSeconds() {
    return TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  @SuppressWarnings("WeakerAccess")
  public static int resolutionInSeconds(String resolutionStr) {
    Matcher matcher = RESOLUTION_PATTERN.matcher(resolutionStr);
    int output = 0;
    while (matcher.find()) {
      output += TimeUnit.MILLISECONDS.toSeconds(convertToMilliseconds(Long.parseLong(matcher.group(1)),
                                                                      matcher.group(2)));
    }
    return output;
  }

  /**
   * Parses a time in String format into a long value. If the input is numeric we assume the input is in seconds.
   *
   * @param timeStr the string to parse
   * @return the parsed time in seconds
   */
  @SuppressWarnings("WeakerAccess")
  public static long parseTimeInSeconds(String timeStr) {
    return parseTime(timeStr, TimeUnit.SECONDS);
  }

  /**
   * Parses a time in String format into a long value
   *
   * @param timeStr the string to parse
   * @param timeUnit the unit of time to return, if timeStr is numeric then it is assumed to be in the unit timeUnit
   * @return the parsed time
   */
  @SuppressWarnings("WeakerAccess")
  public static long parseTime(String timeStr, TimeUnit timeUnit) {
    return parseTime(System.currentTimeMillis(), timeStr, timeUnit);
  }

  /**
   * Parses a time in String format into a long value. If the input is numeric we assume the input is in seconds.
   *
   * @param now the present time in seconds
   * @param timeStr the string to parse
   * @return the parsed time in seconds
   */
  @SuppressWarnings("WeakerAccess")
  public static long parseTimeInSeconds(long now, String timeStr) {
    return parseTime(TimeUnit.MILLISECONDS.convert(now, TimeUnit.SECONDS), timeStr, TimeUnit.SECONDS);
  }

  /**
   * Parses a time in String format into a long value
   *
   * @param now the present time in milliseconds
   * @param timeStr the string to parse
   * @param timeUnit the unit of time to return, if timeStr is numeric then it is assumed to be in the unit timeUnit
   * @return the parsed time
   * @throws IllegalArgumentException if the format of timeStr is bad
   */
  @SuppressWarnings("WeakerAccess")
  public static long parseTime(long now, String timeStr, TimeUnit timeUnit) {
    Preconditions.checkNotNull(timeStr);

    if (NOW.equals(timeStr.toUpperCase())) {
      return now;
    }
    // if its a numeric timestamp, assume units are correct
    Matcher matcher = TIMESTAMP_PATTERN.matcher(timeStr);
    if (matcher.matches()) {
      return Long.parseLong(timeStr);
    }

    // if its some time math pattern like now-1d-6h
    long output = now;
    if (timeStr.startsWith(NOW)) {
      matcher = OP_PATTERN.matcher(timeStr);
      // start at 3 to take into account the NOW at the start of the string
      int prevEndPos = 3;
      while (matcher.find()) {
        // happens if there are unexpected things in-between, like "now 2h-1m"
        if (matcher.start() != prevEndPos) {
          throw new IllegalArgumentException("invalid time format " + timeStr);
        }
        // group 1 should be '+' or '-', group 2 is the number of units, and group 3 is the unit.  ex: 6h
        output += convertToMilliseconds(matcher.group(1), Long.parseLong(matcher.group(2)), matcher.group(3));
        prevEndPos = matcher.end();
      }
      // happens if the end of the string is invalid, like "now-6h 30m"
      if (prevEndPos != timeStr.length()) {
        throw new IllegalArgumentException("invalid time format " + timeStr);
      }
    } else {
      throw new IllegalArgumentException("invalid time format " + timeStr);
    }
    return timeUnit.convert(output, TimeUnit.MILLISECONDS);
  }
}
