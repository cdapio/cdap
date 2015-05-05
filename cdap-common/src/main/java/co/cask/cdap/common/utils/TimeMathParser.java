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

package co.cask.cdap.common.utils;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing time strings into timestamps, with support for some basic time math.
 * Math syntax includes addition and subtraction in minutes, hours, and days.
 * The "NOW" keyword translates to the current epoch time in seconds, and can be strung together with
 * various additions and subtractions.  For example, "NOW" will translate into the current epoch time in seconds.
 * "NOW-5SECONDS" is 5 seconds before now, "NOW-1DAY" is one day before now, and "NOW-1DAY+4HOURS" is 20 hours
 * before now.
 *
 */
public class TimeMathParser {

  private static final String NOW = "now";
  private static final String VALID_UNITS = "s|m|h|d";
  private static final Pattern OP_PATTERN = Pattern.compile("(\\-|\\+)(\\d+)(" + VALID_UNITS + ")");
  private static final Pattern RESOLUTION_PATTERN = Pattern.compile("(\\d+)(" + VALID_UNITS + ")");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^(\\d+)$");

  private static long convertToSeconds(String op, long num, String unitStr) {
    long seconds = 0;
    if ("s".equals(unitStr)) {
      seconds = num;
    } else if ("m".equals(unitStr)) {
      seconds = TimeUnit.MINUTES.toSeconds(num);
    } else if ("h".equals(unitStr)) {
      seconds = TimeUnit.HOURS.toSeconds(num);
    } else if ("d".equals(unitStr)) {
      seconds = TimeUnit.DAYS.toSeconds(num);
    } else {
      throw new IllegalArgumentException("invalid time unit " + unitStr + ", should be one of 's', 'm', 'h', 'd'");
    }

    if ("+".equals(op)) {
      return seconds;
    } else if ("-".equals(op)) {
      return 0 - seconds;
    } else {
      throw new IllegalArgumentException("invalid operation " + op + ", should be either '+' or '-'");
    }
  }

  private static long convertToSeconds(long num, String unitStr) {
    return convertToSeconds("+", num, unitStr);
  }

  public static long nowInSeconds() {
    return TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  public static int resolutionInSeconds(String resolutionStr) {
    Matcher matcher = RESOLUTION_PATTERN.matcher(resolutionStr);
    int output = 0;
    while (matcher.find()) {
      output += convertToSeconds(Long.parseLong(matcher.group(1)), matcher.group(2));
    }
    return output;
  }

  public static long parseTime(String timeStr) {
    return parseTime(nowInSeconds(), timeStr);
  }

  public static long parseTime(long now, String timeStr) {
    Preconditions.checkNotNull(timeStr);

    if (NOW.equals(timeStr.toUpperCase())) {
      return now;
    }
    // if its a timestamp in seconds
    Matcher matcher = TIMESTAMP_PATTERN.matcher(timeStr);
    if (matcher.matches()) {
      return Integer.parseInt(timeStr);
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
        output += convertToSeconds(matcher.group(1), Long.parseLong(matcher.group(2)), matcher.group(3));
        prevEndPos = matcher.end();
      }
      // happens if the end of the string is invalid, like "now-6h 30m"
      if (prevEndPos != timeStr.length()) {
        throw new IllegalArgumentException("invalid time format " + timeStr);
      }
    } else {
      throw new IllegalArgumentException("invalid time format " + timeStr);
    }
    return output;
  }
}
