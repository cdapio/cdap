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

package co.cask.cdap.etl.transform;

import co.cask.cdap.etl.ScriptConstants;

import java.util.Map;

/**
 * Utility methods defined in the JavaScript context of {@link ScriptTransform}.
 * The JavaScript implementation is located in {@link ScriptConstants#HELPER_DEFINITION}.
 */
public interface JavaTypeConverters {
  /**
   * Converts a Java {@link Map} into a JavaScript object.
   *
   * @param map the Java {@link Map}
   * @return the JavaScript object
   */
  Object mapToJSObject(Map<?, ?> map);
}
