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
package co.cask.cdap.internal.app.runtime.schedule;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.quartz.DateBuilder;

/**
 *  Utility class for Scheduler.
 */
public class Schedules {

  /**
   * Converts a frequency expression into cronExpression that is usable by quartz.
   * Supports frequency expressions with the following resolutions: minutes, hours, days.
   * Example conversions:
   * '10m' -> '*{@literal /}10 * * * ?'
   * '3d' -> '0 0 *{@literal /}3 * ?'
   *
   * @return a cron expression
   */
  public static String toCronExpr(String frequency) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(frequency));
    // remove all whitespace
    frequency = frequency.replaceAll("\\s+", "");
    Preconditions.checkArgument(frequency.length() >= 0);

    frequency = frequency.toLowerCase();

    String value = frequency.substring(0, frequency.length() - 1);
    try {
      int parsedValue = Integer.parseInt(value);
      Preconditions.checkArgument(parsedValue > 0);
      // TODO: Check for regular frequency.
      String everyN = String.format("*/%s", value);
      char lastChar = frequency.charAt(frequency.length() - 1);
      switch (lastChar) {
        case 'm':
          DateBuilder.validateMinute(parsedValue);
          return String.format("%s * * * ?", everyN);
        case 'h':
          DateBuilder.validateHour(parsedValue);
          return String.format("0 %s * * ?", everyN);
        case 'd':
          DateBuilder.validateDayOfMonth(parsedValue);
          return String.format("0 0 %s * ?", everyN);
      }
      throw new IllegalArgumentException(String.format("Time unit not supported: %s", lastChar));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Could not parse the frequency");
    }
  }


}
