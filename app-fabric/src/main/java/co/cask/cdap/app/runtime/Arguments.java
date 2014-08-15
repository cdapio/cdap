/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.app.runtime;

import java.util.Map;

/**
 *
 */
public interface Arguments extends Iterable<Map.Entry<String, String>> {

  boolean hasOption(String optionName);

  /**
   * Returns option value for the given option name.
   *
   * @param name Name of the option.
   * @return The value associated with the given name or {@code null} if no such option exists.
   */
  String getOption(String name);

  String getOption(String name, String defaultOption);
}
