/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.securestore.spi.secret;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents metadata for the sensitive data to be stored.
 */
public class SecretMetadata implements Serializable {
  private final String name;
  private final String description;
  private final long creationTimeMs;
  private final Map<String, String> properties;

  private static final long serialVersionUID = -3828981488837592093L;

  /**
   * Constructs metadata with provided secret name, description, creation time and properties.
   *
   * @param name the name of the secret to which this metadata belongs to
   * @param description description of the secret to which this metadata belongs to
   * @param creationTimeMs creation time of the secret in milli seconds
   * @param properties properties of the secret
   */
  public SecretMetadata(String name, String description, long creationTimeMs, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.creationTimeMs = creationTimeMs;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
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
  public long getCreationTimeMs() {
    return creationTimeMs;
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
    return creationTimeMs == that.creationTimeMs &&
      Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, creationTimeMs, properties);
  }

  @Override
  public String toString() {
    return "SecretMetadata{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", creationTimeMs=" + creationTimeMs +
      '}';
  }
}
