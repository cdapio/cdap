/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Provides access to the pipeline arguments at runtime.
 */
public interface Arguments extends Iterable<Map.Entry<String, String>> {

  /**
   * Returns true if the specified argument exists and false if not.
   *
   * @param name the name of the argument
   * @return true if the specified argument exists and false if not
   */
  boolean has(String name);

  /**
   * Returns the value for the specified argument or {@code null} if none exists.
   *
   * @param name the name of the argument
   * @return the value for the specified argument or {@code null} if none exists
   */
  @Nullable
  String get(String name);

}
