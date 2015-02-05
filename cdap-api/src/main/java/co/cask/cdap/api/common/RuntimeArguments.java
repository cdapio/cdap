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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * Utility class to convert String array to Map<String, String>.
 */
public final class RuntimeArguments {

  public static final Map<String, String> NO_ARGUMENTS = Collections.emptyMap();

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
   * Extract all arguments for a given scope.
   * @param scope The type of the scope
   * @param name The name of the scope, e.g. "myTable"
   * @param arguments the runtime arguments of the enclosing scope
   * @return a map that contains only the keys that start with &lt;scope>.&lt;name>., with that prefix removed.
   */
  public static Map<String, String> extractScope(Scope scope, String name, Map<String, String> arguments) {
    String prefix = scope + "." + name + ".";
    return extractPrefix(prefix, arguments);
  }

  /**
   * Extract all arguments that start with a prefix, and removes that prefix.
   * @param prefix The prefix to filter and remove
   * @param arguments the runtime arguments to extract from
   * @return a map that contains only the keys that start with the prefix, with the prefix removed.
   */
  public static Map<String, String> extractPrefix(String prefix, Map<String, String> arguments) {
    if (arguments == null || arguments.isEmpty()) {
      return arguments;
    }
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> entry : arguments.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
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

}
