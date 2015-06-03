/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Constants and helper methods to configure runtime arguments for a file dataset.
 */
public class FileSetArguments {

  /**
   * The paths of the files to be read. Specified as a runtime argument for the dataset.
   * Each path is relative to the dataset's base path, and multiple paths can be given,
   * separated by commas.
   */
  public static final String INPUT_PATHS = "input.paths";

  /**
   * The path of the file to write. Specified as a runtime argument for the dataset.
   * The path is relative to the dataset's base path.
   */
  public static final String OUTPUT_PATH = "output.path";

  /**
   * Sets the input path in the runtime arguments for a file dataset.
   */
  public static void setInputPath(Map<String, String> arguments, String path) {
    arguments.put(INPUT_PATHS, path);
  }

  /**
   * Sets multiple input paths in the runtime arguments for a file dataset.
   * @param paths A comma-separated sequence of paths
   */
  public static void setInputPaths(Map<String, String> arguments, String paths) {
    arguments.put(INPUT_PATHS, paths);
  }

  /**
   * Sets multiple input paths in the runtime arguments for a file dataset.
   */
  public static void setInputPaths(Map<String, String> arguments, Collection<String> paths) {
    arguments.remove(INPUT_PATHS);
    for (String path : paths) {
      addInputPath(arguments, path);
    }
  }

  /**
   * Add an input path in the runtime arguments for a file dataset.
   */
  public static void addInputPath(Map<String, String> arguments, String path) {
    String existing = arguments.get(INPUT_PATHS);
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
    arguments.put(OUTPUT_PATH, path);
  }

  /**
   * @return the output path in the runtime arguments for a file dataset.
   */
  public static String getOutputPath(Map<String, String> arguments) {
    return arguments.get(OUTPUT_PATH);
  }

  /**
   * @return the output path in the runtime arguments for a file dataset.
   */
  public static Collection<String> getInputPaths(Map<String, String> arguments) {
    String pathsArg = arguments.get(INPUT_PATHS);
    if (pathsArg == null) {
      return Collections.emptyList();
    }
    Iterable<String> paths = Splitter.on(',').omitEmptyStrings().trimResults().split(pathsArg);
    return Lists.newArrayList(paths);
  }

}
