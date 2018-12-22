/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.securestore.spi;

import java.util.Map;

/**
 *
 */
public class SecureDataMetadata {
  private String name;
  private String description;
  private long createTimeMs;
  private Map<String, String> properties;

  /**
   *
   * @param name
   * @param description
   * @param createTimeMs
   * @param properties
   */
  public SecureDataMetadata(String name, String description, long createTimeMs, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.createTimeMs = createTimeMs;
    this.properties = properties;
  }

  /**
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   *
   * @return
   */
  public String getDescription() {
    return description;
  }

  /**
   *
   * @return
   */
  public long getCreateTimeMs() {
    return createTimeMs;
  }

  /**
   *
   * @return
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
