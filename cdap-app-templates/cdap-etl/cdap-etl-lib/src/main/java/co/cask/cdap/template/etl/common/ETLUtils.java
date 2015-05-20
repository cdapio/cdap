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

package co.cask.cdap.template.etl.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for ETL
 */
public final class ETLUtils {

  /**
   * Parses a duration String to its long value.
   * Frequency string consists of a number followed by an unit, with 's' for seconds, 'm' for minutes, 'h' for hours
   * and 'd' for days. For example, an input of '5m' means 5 minutes which will be parsed to 300000 milliseconds.
   *
   * @param durationStr the duration string (ex: 5m, 5h etc).
   * @return long which is milliseconds equivalent of the duration string
   */
  public static long parseDuration(String durationStr) {
    //TODO: replace with TimeMathParser (available only internal to cdap)
    Preconditions.checkArgument(!Strings.isNullOrEmpty(durationStr));
    durationStr = durationStr.toLowerCase();

    String value = durationStr.substring(0, durationStr.length() - 1);
    int parsedValue = 0;
    try {
      parsedValue = Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      Throwables.propagate(nfe);
    }
    Preconditions.checkArgument(parsedValue >= 0);

    char lastChar = durationStr.charAt(durationStr.length() - 1);
    switch (lastChar) {
      case 's':
        return TimeUnit.SECONDS.toMillis(parsedValue);
      case 'm':
        return TimeUnit.MINUTES.toMillis(parsedValue);
      case 'h':
        return TimeUnit.HOURS.toMillis(parsedValue);
      case 'd':
        return TimeUnit.DAYS.toMillis(parsedValue);
    }
    throw new IllegalArgumentException(String.format("Time unit not supported: %s", lastChar));
  }

  private ETLUtils() {
    throw new AssertionError("Suppress default constructor for non-instantiability");
  }
}
