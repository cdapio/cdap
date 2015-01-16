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

package co.cask.cdap.api.dataset.lib;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helpers for manipulating runtime arguments of time-partitioned file sets.
 */
public class TimePartitionedFileSetArguments {

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
   * @return the time of the outout partition to be written
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
