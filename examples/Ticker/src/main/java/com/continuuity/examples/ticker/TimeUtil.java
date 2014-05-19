/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for working with time. Methods to convert date strings from an expected timezone to timestamps
 * and methods for convert a string of the format "now-Xunits" to its corresponding timestamp.
 */
public class TimeUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TimeUtil.class);

  // Pattern for parsing time in query params such as end=now-5s
  private static final Pattern TIME_PATTERN = Pattern.compile("^now\\-(\\d+)(s|m|h|d)$");
  private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^(\\d+)$");
  private static final DateFormat dateFormat = getEDTDateFormat();
  private static final DateFormat netFondsDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss");

  static {
    netFondsDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
  }

  /**
   * Convert 's' into seconds, 'm' into minutes, 'h' into hours', and 'd' into days.
   */
  private static TimeUnit convert(String unitStr) {
    if ("s".equals(unitStr)) {
      return TimeUnit.SECONDS;
    } else if ("m".equals(unitStr)) {
      return TimeUnit.MINUTES;
    } else if ("h".equals(unitStr)) {
      return TimeUnit.HOURS;
    } else if ("d".equals(unitStr)) {
      return TimeUnit.DAYS;
    } else {
      throw new IllegalArgumentException("invalid time unit, must be 's', 'm', 'h', or 'd'");
    }
  }

  /**
   * Get the current time in seconds.
   */
  public static long nowInSeconds() {
    return TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  public static long parseTime(String timeStr) {
    return parseTime(nowInSeconds(), timeStr);
  }

  /**
   * Parses a time value that must either be a timestamp -- or something of the form
   * now(-|+)X(s|m|h|d) where X is some number -- into epoch time in seconds.  For example:
   * now-5s translates to 5 seconds before the current time.
   */
  public static long parseTime(long now, String timeStr) {
    if ("now".equals(timeStr)) {
      return now;
    }
    Matcher matcher = TIMESTAMP_PATTERN.matcher(timeStr);
    if (matcher.matches()) {
      return Integer.parseInt(timeStr);
    }
    matcher = TIME_PATTERN.matcher(timeStr);
    if (matcher.matches()) {
      String unit = matcher.group(2);
      long offset = convert(unit).toSeconds(Long.parseLong(matcher.group(1)));
      return now - offset;
    }
    LOG.error("invalid time format");
    throw new IllegalArgumentException("invalid time format");
  }

  public static String timestampToDate(long timestamp) {
    return dateFormat.format(new Date(1000 * timestamp)).toString();
  }

  public static DateFormat getEDTDateFormat() {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TimeZone.getTimeZone("EDT"));
    return df;
  }

  public static long netfondsDateToTimestamp(String dateStr) throws ParseException {
    return TimeUnit.SECONDS.convert(netFondsDateFormat.parse(dateStr).getTime(), TimeUnit.MILLISECONDS);
  }
}
