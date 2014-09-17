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

import java.util.Map;

/**
 * Represents classes that provides properties.
 */
public interface PropertyProvider {

  /**
   * Returns an immutable Map of all properties.
   */
  Map<String, String> getProperties();

  /**
   * Return the property value of a given key.
   * @param key for getting specific property value.
   * @return The value associated with the key or {@code null} if not such key exists.
   */
  String getProperty(String key);
}
