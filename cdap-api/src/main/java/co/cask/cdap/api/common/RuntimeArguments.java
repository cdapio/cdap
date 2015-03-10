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

package co.cask.cdap.api.common;

import co.cask.cdap.api.schedule.Schedule;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to convert String array to Map<String, String>.
 */
public final class RuntimeArguments {

  public static final Map<String, String> NO_ARGUMENTS = Collections.emptyMap();
  private static final String ASTERISK = "*";
  private static final String DOT = ".";

  private RuntimeArguments() {
  }

  /**
   * Converts a POSIX compliant program argument array to a String-to-String Map.
   * @param args Array of Strings where each element is a POSIX compliant program argument (Ex: "--os=Linux" ).
   * @return Map of argument Keys and Values (Ex: Key = "os" and Value = "Linux").
   */
  public static Map<String, String> fromPosixArray(String[] args) {
    Map<String, String> kvMap = Maps.newHashMap();
    for (String arg : args) {
      kvMap.putAll(Splitter.on("--").omitEmptyStrings().trimResults().withKeyValueSeparator("=").split(arg));
    }
    return kvMap;
  }

  /**
   * Converts a String-to-String Map to POSIX compliant program argument array.
   * @param kvMap Map of argument Keys and Values.
   * @return Array of Strings in POSIX compliant format.
   */
  public static String[] toPosixArray(Map<String, String> kvMap) {
    String[] args = new String[kvMap.size()];
    int index = 0;
    for (Map.Entry<String, String> kv : kvMap.entrySet()) {
      args[index++] = String.format("--%s=%s", kv.getKey(), kv.getValue());
    }
    return args;
  }

  /**
   * Converts a Iterable of Map.Entry<String, String> to a POSIX compliant program argument array.
   * @param iterable of Map.Entry of argument Key and Value.
   * @return Array of Strings in POSIX compliant format.
   */
  public static String[] toPosixArray(Iterable<Map.Entry<String, String>> iterable) {
    ImmutableMap.Builder<String, String> userArgMapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> kv : iterable) {
      userArgMapBuilder.put(kv);
    }
    return toPosixArray(userArgMapBuilder.build());
  }

  /**
   * Identifies arguments with a given scope prefix and adds them back without the scope prefix.
   *
   * 1. An argument can be prefixed by "&lt;scope>.&lt;name>.". e.g. mapreduce.myMapReduce.read.timeout=30. In this case
   * the MapReduce program named 'myMapReduce' will receive two arguments - mapreduce.myMapReduce.read.timeout=30 and
   * read.timeout=30. However MapReduce programs other than 'myMapReduce' will receive only one argument -
   * mapreduce.myMapReduce.read.timeout=30
   * 2. An argument can be prefixed by "&lt;scope>.*.". e.g. mapreduce.*.read.timeout=30. In this case all the
   * underlying MapReduce programs will receive the arguments mapreduce.*.read.timeout=30 and read.timeout=30.
   * 3. An argument not prefixed with any scope is passed further without any changes. e.g. read.timeout=30
   *
   * @param scope The type of the scope
   * @param name The name of the scope, e.g. "myTable"
   * @param arguments the runtime arguments of the enclosing scope
   * @return a map that contains the arguments with and without prefix
   */
  public static Map<String, String> extractScope(Scope scope, String name, Map<String, String> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return arguments;
    }

    String prefix = scope + DOT + name + DOT;
    String wildCardPrefix = scope + DOT + ASTERISK + DOT;

    Map<String, String> result = Maps.newHashMap();
    result.putAll(arguments);

    Map<String, String> prefixMatchedArgs = Maps.newHashMap();
    Map<String, String> wildCardPrefixMatchedArgs = Maps.newHashMap();

    // Group the arguments into different categories based on wild card prefix match, named prefix match or no match
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      if (entry.getKey().startsWith(wildCardPrefix)) {
        // Argument is prefixed with "<scope>.*."
        wildCardPrefixMatchedArgs.put(entry.getKey().substring(wildCardPrefix.length()), entry.getValue());
      } else if (entry.getKey().startsWith(prefix)) {
        // Argument is prefixed with "<scope>.<name>."
        prefixMatchedArgs.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }

    result.putAll(wildCardPrefixMatchedArgs);
    result.putAll(prefixMatchedArgs);
    return result;
  }

  /**
   * Add a scope prefix to all arguments.
   * @param scope The type of the scope
   * @param name The name of the scope, e.g. "myTable"
   * @param arguments the runtime arguments to be scoped
   * @return a map that contains all keys, prefixed with with &lt;scope>.&lt;name>.
   */
  public static Map<String, String> addScope(Scope scope, String name, Map<String, String> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return arguments;
    }
    final String prefix = scope + "." + name + ".";
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
        result.put(prefix + entry.getKey(), entry.getValue());
    }
    return result;
  }

  private static final Pattern DATE_PATTERN = Pattern.compile("\\$\\{([^\\{\\}]+)\\}");

  private static final Pattern OFFSET_PATTERN = Pattern.compile(":\\$\\{(.*)\\}$");

  /**
   * Resolves the {@link Schedule} arguments based on the rules specified in the argument values and
   * logical start time of the {@link Schedule}. Rules specified in the property values must be enclosed
   * by "${" and "}" and should conform to the patterns specified by {@link SimpleDateFormat}.
   *
   * Example: these properties are specified for a {@link Schedule}:
   * <p>
   *   input.file=purchase_data_${MMddyyyy}_${Ha}.input
   *   output.file=purchase_data_${dEyyyy}.output
   * </p>
   *
   * If the {@link Schedule} triggers at a logical start time of '1422957600' (February 3, 2015 at 2:00:00 AM),
   * the rules in the {@link Schedule} arguments will be evaluated and these properties will be passed to
   * the triggered program:
   * <p>
   *   input.file=purchase_data_02032015_2AM.input
   *   output.file=purchase_data_3Tue2015.output
   * </p>
   *
   * To resolve the rules against a time other than the logical start time of the {@link Schedule},
   * provide an offset in the rules. The offset should be provided at the end of the property value,
   * enclosed by ":{" and "}".
   *
   * Example: this property is specified for a {@link Schedule}:
   * <p>
   *   input.file=purchase_data.${MMM dd HH:mm:ss}.input:${-30s,-15m,-2h,3d}
   * </p>
   *
   * The offsets specified for the above property are -30s (30 seconds prior to the logical start time), -15m
   * (15 minutes prior to the logical start time), -2h (2 hours prior to the logical start time) and 3d (3 days
   * after the logical start time).
   *
   * If the {@link Schedule} triggers at a logical start time of '1422957600' (February 3, 2015 at 2:00:00 AM),
   * the rule will be evaluated for each offset and comma-separated values for each will be passed to the triggered
   * program:
   * <p>
   *   input.file=purchase_data.Feb 03 01:59:30.input,purchase_data.Feb 03 01:45:00.input,
   *              purchase_data.Feb 03 00:00:00.input,purchase_data.Feb 06 02:00:00.input
   * </p>
   *
   * @param arguments the arguments for the Schedule
   * @param logicalStartTime the logical start time for the Schedule in seconds
   * @return the map of resolved Schedule properties
   */
  public static Map<String, String> resolveScheduleArguments(Map<String, String> arguments, long logicalStartTime) {
    Map<String, String> result = Maps.newHashMap();

    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      result.put(entry.getKey(), getResolvedArgument(entry.getValue(), logicalStartTime));
    }
    return result;
  }

  private static String getResolvedArgument(String argumentValue, long logicalStartTime) {
    List<Long> timeOffsets = new ArrayList<Long>();

    // Extract the offsets from the argument value and store all offsets in the timeOffsets list
    String argumentWithoutOffset = extractOffsets(argumentValue, timeOffsets, logicalStartTime);
    StringBuffer result = new StringBuffer();

    // Resolve the rule for each offset
    for (int i = 0; i < timeOffsets.size(); i++) {
      Matcher datePatternMatcher = DATE_PATTERN.matcher(argumentWithoutOffset);
      while (datePatternMatcher.find()) {
        datePatternMatcher.appendReplacement(result, getReplacement(datePatternMatcher.group(1), timeOffsets.get(i)));
      }
      datePatternMatcher.appendTail(result);
      if (result.toString().equals(argumentWithoutOffset) && timeOffsets.size() > 1) {
        throw new IllegalArgumentException("Rules are missing for the property " + argumentValue);
      }
      // Add ',' at the end of each resolved value
      if (i != timeOffsets.size() - 1) {
        result.append(",");
      }
    }
    return result.toString();
  }

  private static String extractOffsets(String argumentValue, List<Long> timeOffsets, long logicalStartTime) {
    timeOffsets.add(logicalStartTime);
    StringBuffer sb = new StringBuffer();
    Matcher offsetMatcher = OFFSET_PATTERN.matcher(argumentValue);
    if (offsetMatcher.find()) {
      // Offset is specified in the argument value
      String offsetString = offsetMatcher.group(1);
      // Split the ',' separated offset string into individual offsets.
      for (String offset : offsetString.split(",")) {
        try {
          // Offset ends with offset unit - 's', 'm', 'h' or 'd'. Find the time component and offset unit component
          // from the offset.
          Long offsetValue = Long.valueOf(offset.substring(0, offset.length() - 1).trim());
          if (offset.endsWith("s")) {
            timeOffsets.add(logicalStartTime + offsetValue);
          } else if (offset.endsWith("m")) {
            timeOffsets.add(logicalStartTime + offsetValue * 60);
          } else if (offset.endsWith("h")) {
            timeOffsets.add(logicalStartTime + offsetValue * 3600);
          } else if (offset.endsWith("d")) {
            timeOffsets.add(logicalStartTime + offsetValue * 24 * 3600);
          } else {
            // Invalid offset unit specified. Log the error.
            throw new IllegalArgumentException("Offset for the property must end with 's', 'm', 'h' or 'd'."
                                                 + "Invalid offset specified for the Schedule property "
                                                 + argumentValue);
          }
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException("Invalid offset specified for the Schedule property " + argumentValue
                                               + " Reason: " + ex.getCause());
        }
      }
      offsetMatcher.appendReplacement(sb, "");
    } else {
      // Offset is not specified
      sb.append(argumentValue);
    }
    return sb.toString();
  }

  private static String getReplacement(String format, long time) {
    SimpleDateFormat formatter = new SimpleDateFormat(format);
    // convert time in seconds into milliseconds before formatting
    return formatter.format(time * 1000);
  }
}
