/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.security.securestore;

import com.google.gson.Gson;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the metadata for the data stored in the Secure Store.
 */
public final class SecureStoreMetadata {

  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String CREATED_FIELD = "createdEpochMs";
  private static final String PROPERTIES_FIELD = "properties";
  private static final String DESCRIPTION_DEFAULT = "";
  private static final Gson GSON = new Gson();

  private final String name;
  private final String description;
  private final long createdEpochMs;
  private final Map<String, String> properties;

  private SecureStoreMetadata(String name, String description, Date created, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.createdEpochMs = created.getTime();
    this.properties = properties;
  }

  public static SecureStoreMetadata of(String name, Map<String, String> properties) {
    String tempDescription = properties.get(DESCRIPTION_FIELD) == null ?
      DESCRIPTION_DEFAULT : properties.get(DESCRIPTION_FIELD);

    return new SecureStoreMetadata(name, tempDescription, new Date(), properties);
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

  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return "SecureStoreMetadata{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", createdEpochMs=" + createdEpochMs +
      ", properties=" + properties +
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
    return Objects.hashCode(this);
  }
}
