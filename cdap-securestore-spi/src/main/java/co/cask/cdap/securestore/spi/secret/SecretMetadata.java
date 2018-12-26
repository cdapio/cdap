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

package co.cask.cdap.securestore.spi.secret;

import java.util.Map;
import java.util.Objects;

/**
 * Represents metadata for the sensitive data to be stored.
 */
public class SecretMetadata {
  private String name;
  private String description;
  private long createTimeMs;
  private Map<String, String> properties;

  /**
   * Constructs metadata with provided secret name, description, creation time and properties.
   *
   * @param name the name of the secret to which this metadata belongs to
   * @param description description of the secret to which this metadata belongs to
   * @param createTimeMs creation time of the secret in milli seconds
   * @param properties properties of the secret
   */
  public SecretMetadata(String name, String description, long createTimeMs, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.createTimeMs = createTimeMs;
    this.properties = properties;
  }

  /**
   * @return name of the secret
   */
  public String getName() {
    return name;
  }

  /**
   * @return description of the secret
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return creation time of the secret in milli seconds
   */
  public long getCreateTimeMs() {
    return createTimeMs;
  }

  /**
   * @return properties of the secret
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecretMetadata that = (SecretMetadata) o;
    return createTimeMs == that.createTimeMs &&
      Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, createTimeMs, properties);
  }
}
