/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;

/**
 * Helpers for manipulating runtime arguments of time-partitioned file sets.
 */
@Beta
public class TimePartitionedFileSetArguments extends PartitionedFileSetArguments {

  public static final String OUTPUT_PATH_FORMAT = "output.file.path.format";
  public static final String OUTPUT_TIME_ZONE = "output.time.zone";
  public static final String OUTPUT_PARTITION_TIME = "output.partition.time";
  public static final String INPUT_START_TIME = "input.start.time";
  public static final String INPUT_END_TIME = "input.end.time";

  /**
   * Set the time of the output partition when using TimePartitionedFileSet as an OutputFormatProvider.
   * This time is used as the partition key for the new file, and also to generate an output file path - if that path
   * is not explicitly given as an argument itself.
   * @param time The time in milli seconds.
   */
  public static void setOutputPartitionTime(Map<String, String> arguments, long time) {
    arguments.put(OUTPUT_PARTITION_TIME, Long.toString(time));
  }

  /**
   * @return the time of the output partition to be written
   */
  @Nullable
  public static Long getOutputPartitionTime(Map<String, String> arguments) {
    String str = arguments.get(OUTPUT_PARTITION_TIME);
    if (str == null) {
      return null;
    }
    return Long.parseLong(str);
  }

  /**
   * This is the file extension for each partition when using TimePartitionedFileSet as an OutputFormatProvider.
   * It's used to generate the end of the output file path for each partition.
   * @param pathFormat The format for the path; for example: 'yyyy-MM-dd/HH-mm,America/Los_Angeles',
   *                   which will create a file path ending in the format of 2015-01-01/20-42,
   *                   with the time of the partition being the time in the timezone of Los Angeles (PST or PDT).
   *                   The pathFormat will be the format provided to
   *                   {@link java.text.SimpleDateFormat}. If left blank, then the partitions will be of the form
   *                   2015-01-01/20-42.142017372000, with the time being the time UTC.
   *                   Note that each partition must have a unique file path or a runtime exception will be thrown.
   */
  public static void setOutputPathFormat(Map<String, String> arguments, String pathFormat) {
    setOutputPathFormat(arguments, pathFormat, null);
  }

  /**
   * This is the file extension for each partition when using TimePartitionedFileSet as an OutputFormatProvider.
   * It's used to generate the end of the output file path for each partition.
   * @param pathFormat The format for the path; for example: 'yyyy-MM-dd/HH-mm,America/Los_Angeles',
   *                   which will create a file path ending in the format of 2015-01-01/20-42,
   *                   with the time of the partition being the time in the timezone of Los Angeles (PST or PDT).
   *                   The pathFormat will be the format provided to
   *                   {@link java.text.SimpleDateFormat}. If left blank, then the partitions will be of the form
   *                   2015-01-01/20-42.142017372000, with the time being the time UTC.
   *                   Note that each partition must have a unique file path or a runtime exception will be thrown.
   * @param timeZone The string ID of the time zone. It is parsed by {@link TimeZone#getTimeZone(String)},
   *                 and if the string ID is not a valid time zone, UTC is used.
   */
  public static void setOutputPathFormat(Map<String, String> arguments, String pathFormat, @Nullable String timeZone) {
    long curTime = System.currentTimeMillis();
    boolean hasTimeZone = timeZone != null && !timeZone.isEmpty();
    try {
      SimpleDateFormat format = new SimpleDateFormat(pathFormat);
      if (hasTimeZone) {
        format.setTimeZone(TimeZone.getTimeZone(timeZone));
      }
      format.format(new Date(curTime));
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid date format: " + pathFormat + '\n' + e);
    }
    arguments.put(OUTPUT_PATH_FORMAT, pathFormat);
    if (hasTimeZone) {
      arguments.put(OUTPUT_TIME_ZONE, timeZone);
    }
  }

  /**
   * This is the file extension for each partition when using TimePartitionedFileSet as an OutputFormatProvider.
   * It's used to generate the end of the output file path for each partition.
   * May be null.
   */
  @Nullable
  public static String getOutputPathFormat(Map<String, String> arguments) {
    return arguments.get(OUTPUT_PATH_FORMAT);
  }

  /**
   * This is the time zone used to format the date for the output partition.
   * @return The String ID of the time zone of the date.
   * May be null.
   */
  @Nullable
  public static String getOutputPathTimeZone(Map<String, String> arguments) {
    return arguments.get(OUTPUT_TIME_ZONE);
  }

  /**
   * Set the start (inclusive) of the time range to be read as input.
   * @param time The time in milli seconds.
   */
  public static void setInputStartTime(Map<String, String> arguments, long time) {
    arguments.put(INPUT_START_TIME, Long.toString(time));
  }

  /**
   * Get the start (inclusive) of the time range to be read as input.
   */
  @Nullable
  public static Long getInputStartTime(Map<String, String> arguments) {
    String str = arguments.get(INPUT_START_TIME);
    if (str == null) {
      return null;
    }
    return Long.parseLong(str);
  }

  /**
   * Set the end (exclusive) of the time range to be read as input.
   * @param time The time in milli seconds.
   */
  public static void setInputEndTime(Map<String, String> arguments, long time) {
    arguments.put(INPUT_END_TIME, Long.toString(time));
  }

  /**
   * Get the end (exclusive) of the time range to be read as input.
   */
  public static Long getInputEndTime(Map<String, String> arguments) {
    String str = arguments.get(INPUT_END_TIME);
    if (str == null) {
      return null;
    }
    return Long.parseLong(str);
  }

}
