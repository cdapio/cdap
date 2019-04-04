/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.security.store;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the metadata for the data stored in the Secure Store.
 */
public final class SecureStoreMetadata {

  private final String name;
  private final String description;
  private final long createdEpochMs;
  private final Map<String, String> properties;

  public SecureStoreMetadata(String name, @Nullable String description, long created, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.createdEpochMs = created;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  /**
   * @return Name of the data.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Last time, in epoch, this element was modified.
   */
  public long getLastModifiedTime() {
    return createdEpochMs;
  }

  /**
   * @return A map of properties associated with this element.
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return "SecureStoreMetadata{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", createdEpochMs=" + createdEpochMs +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecureStoreMetadata that = (SecureStoreMetadata) o;
    return name.equals(that.name) && description.equals(that.description) && createdEpochMs == that.createdEpochMs
      && (properties != null ? properties.equals(that.properties) : that.properties == null);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, createdEpochMs, properties);
  }
}
