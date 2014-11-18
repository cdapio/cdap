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

package co.cask.cdap.cli.util;

import co.cask.cdap.cli.exception.CommandInputError;

import java.util.concurrent.TimeUnit;

/**
 * Helper class with static methods that is used by the CLI
 */
public class TimestampUtil {

  /**
   * Returns a timestamp in milliseconds.
   *
   * @param arg The string argument user provided.
   * @param base The base timestamp to relative from if the time format provided is a relative time.
   * @return Timestamp in milliseconds
   * @throws co.cask.cdap.cli.exception.CommandInputError if failed to parse input.
   */
  public static long getTimestamp(String arg, long base) {
    try {
      if (arg.startsWith("+") || arg.startsWith("-")) {
        int dir = arg.startsWith("+") ? 1 : -1;
        char type = arg.charAt(arg.length() - 1);
        int offset = Integer.parseInt(arg.substring(1, arg.length() - 1));
        switch (type) {
          case 's':
            return base + dir * TimeUnit.SECONDS.toMillis(offset);
          case 'm':
            return base + dir * TimeUnit.MINUTES.toMillis(offset);
          case 'h':
            return base + dir * TimeUnit.HOURS.toMillis(offset);
          case 'd':
            return base + dir * TimeUnit.DAYS.toMillis(offset);
          default:
            throw new CommandInputError("Unsupported relative time format: " + type);
        }
      }
      if (arg.equalsIgnoreCase("min")) {
        return 0L;
      }
      if (arg.equalsIgnoreCase("max")) {
        return Long.MAX_VALUE;
      }

      return Long.parseLong(arg);
    } catch (NumberFormatException e) {
      throw new CommandInputError("Invalid number value: " + arg + ". Reason: " + e.getMessage());
    }
  }

}
