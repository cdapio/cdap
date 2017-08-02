/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.etl.api.action;

import co.cask.cdap.etl.api.Arguments;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Arguments that can be updated. Arguments set when preparing a run will be visible to all pipeline stages
 * at execution time, and will be visible to any stages after the current one. Arguments can also
 * overwrite each other, so plugins should prefix their arguments by their stage name if they don't want the argument
 * to be clobbered by other instances of the plugin.
 */
public interface SettableArguments extends Arguments {

  /**
   * Returns true if specified argument is provided, otherwise false is returned.
   */
  boolean has(String name);

  /**
   * Returns the value for the given argument name if it exist, otherwise {@code null} is returned.
   */
  @Nullable
  String get(String name);

  /**
   * Sets the name and value as specified by the input parameters.
   */
  void set(String name, String value);

  /**
   * Returns an unmodifiable map that represents all arguments.
   */
  Map<String, String> asMap();

}
