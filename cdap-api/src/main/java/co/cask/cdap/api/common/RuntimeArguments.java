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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
    Map<String, String> kvMap = new HashMap<>();
    for (String arg : args) {
      int idx = arg.indexOf('=');
      int keyOff = arg.startsWith("--") ? "--".length() : 0;
      String key = idx < 0 ? arg.substring(keyOff) : arg.substring(keyOff, idx);
      String value = idx < 0 ? "" : arg.substring(idx + 1);
      kvMap.put(key, value);
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
    Map<String, String> userArgs = new HashMap<>();
    for (Map.Entry<String, String> kv : iterable) {
      userArgs.put(kv.getKey(), kv.getValue());
    }
    return toPosixArray(userArgs);
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
   * @return a new map that contains the arguments with and without prefix, never null
   */
  public static Map<String, String> extractScope(Scope scope, String name, Map<String, String> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return new HashMap<>();
    }

    String prefix = scope + DOT + name + DOT;
    String wildCardPrefix = scope + DOT + ASTERISK + DOT;

    Map<String, String> result = new HashMap<>();
    result.putAll(arguments);

    Map<String, String> prefixMatchedArgs = new HashMap<>();
    Map<String, String> wildCardPrefixMatchedArgs = new HashMap<>();

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
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
        result.put(prefix + entry.getKey(), entry.getValue());
    }
    return result;
  }

}
