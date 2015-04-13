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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An Utility class for ETL
 */
public class ETLUtils {

  /**
   * Parses a frequency String to its long value.
   * Frequency string consists of a number followed by an unit, with 's' for seconds, 'm' for minutes, 'h' for hours
   * and 'd' for days. For example, an input of '5m' means 5 minutes which will be parsed to 300000 milliseconds.
   *
   * @param frequencyStr the frequency string (ex: 5m, 5h etc).
   * @return long which is milliseconds equivalent of the frequency string
   */
  public static long parseFrequency(String frequencyStr) {
    //TODO: replace with TimeMathParser (available only internal to cdap)
    Preconditions.checkArgument(!Strings.isNullOrEmpty(frequencyStr));
    frequencyStr = frequencyStr.toLowerCase();

    String value = frequencyStr.substring(0, frequencyStr.length() - 1);
    int parsedValue = 0;
    try {
      parsedValue = Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      Throwables.propagate(nfe);
    }
    Preconditions.checkArgument(parsedValue > 0);

    char lastChar = frequencyStr.charAt(frequencyStr.length() - 1);
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

  /**
   * Builds a {@link Partitioning} which is used to set the partitioning for
   * {@link PartitionedFileSet}
   *
   * @param partitioningString a JSON format partitioning string
   * @return {@link Partitioning}
   */
  public static Partitioning createPartitioning(String partitioningString) {
    Partitioning.Builder builder = Partitioning.builder();
    Type stringStringMap = new TypeToken<Map<String, String>>() {
    }.getType();
    Map<String, String> filters = new Gson().fromJson(partitioningString, stringStringMap);
    for (Map.Entry<String, String> entrySet : filters.entrySet()) {
      builder.addField(entrySet.getKey(), Partitioning.FieldType.valueOf(entrySet.getValue()));
    }
    return builder.build();
  }
}
