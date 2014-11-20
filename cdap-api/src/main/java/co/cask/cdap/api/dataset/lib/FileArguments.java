/*
 * Copyright 2014 Cask Data, Inc.
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

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Constants and helper methods to configure runtime arguments for a file dataset.
 */
public class FileArguments {

  /**
   * Sets the input path in the runtime arguments for a file dataset.
   */
  public static void setInputPath(Map<String, String> arguments, String path) {
    arguments.put(File.ARGUMENT_INPUT_PATHS, path);
  }

  /**
   * Sets multiple input paths in the runtime arguments for a file dataset.
   * @param paths A comma-separated sequence of paths
   */
  public static void setInputPaths(Map<String, String> arguments, String paths) {
    arguments.remove(File.ARGUMENT_INPUT_PATHS);
    for (String path : paths.split(",")) {
      addInputPath(arguments, path.trim());
    }
  }

  /**
   * Sets multiple input paths in the runtime arguments for a file dataset.
   */
  public static void setInputPaths(Map<String, String> arguments, Collection<String> paths) {
    arguments.remove(File.ARGUMENT_INPUT_PATHS);
    for (String path : paths) {
      addInputPath(arguments, path);
    }
  }

  /**
   * Add an input path in the runtime arguments for a file dataset.
   */
  public static void addInputPath(Map<String, String> arguments, String path) {
    String existing = arguments.get(File.ARGUMENT_INPUT_PATHS);
    if (existing == null) {
      setInputPath(arguments, path);
    } else {
      setInputPath(arguments, existing + "," + path);
    }
  }

  /**
   * Sets the output path in the runtime arguments for a file dataset.
   */
  public static void setOutputPath(Map<String, String> arguments, String path) {
    arguments.put(File.ARGUMENT_OUTPUT_PATH, path);
  }

  /**
   * @return the output path in the runtime arguments for a file dataset.
   */
  public static String getOutputPath(Map<String, String> arguments) {
    return arguments.get(File.ARGUMENT_OUTPUT_PATH);
  }

  /**
   * @return the output path in the runtime arguments for a file dataset.
   */
  public static Collection<String> getInputPaths(Map<String, String> arguments) {
    String pathsArg = arguments.get(File.ARGUMENT_INPUT_PATHS);
    if (pathsArg == null) {
      return Collections.emptyList();
    }
    String[] paths = pathsArg.split(",");
    List<String> result = Lists.newArrayListWithCapacity(paths.length);
    for (String path : paths) {
      result.add(path.trim());
    }
    return result;
  }

}
