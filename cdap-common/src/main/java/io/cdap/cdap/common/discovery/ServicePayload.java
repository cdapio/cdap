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

package co.cask.cdap.common.discovery;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * Defines the payload that is placed for every service.
 */
public class ServicePayload {
  private final Map<String, String> values = Maps.newHashMap();

  /**
   * Adds a key and value as service payload.
   *
   * @param key to be stored.
   * @param value to be associated with key.
   */
  public void add(String key, String value) {
    values.put(key, value);
  }

  public String get(String key) {
    return values.get(key);
  }

  public Set<Map.Entry<String, String>> getAll() {
    return values.entrySet();
  }
}

