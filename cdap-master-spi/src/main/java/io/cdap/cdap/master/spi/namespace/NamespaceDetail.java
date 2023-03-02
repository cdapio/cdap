/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.namespace;

import java.util.Map;

/**
 * CDAP namespace details.
 */
public class NamespaceDetail {

  private final String name;
  private final Map<String, String> properties;

  public NamespaceDetail(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  /**
   * Returns the name of the CDAP namespace.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns namespace properties.
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
