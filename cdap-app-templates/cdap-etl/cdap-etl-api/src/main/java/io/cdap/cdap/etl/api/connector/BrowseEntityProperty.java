/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import java.util.Objects;

/**
 * Object that represents an explore entity property.
 */
public class BrowseEntityProperty {

  /**
   * The property type for the browse entity
   */
  public enum PropertyType {
    STRING,
    DATE,
    TIMESTAMP_MILLIS,
    NUMBER
  }

  private final String key;
  private final String value;
  private final PropertyType type;

  public BrowseEntityProperty(String key, String value, PropertyType type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public PropertyType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrowseEntityProperty that = (BrowseEntityProperty) o;
    return Objects.equals(key, that.key) &&
      Objects.equals(value, that.value) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, type);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder(String name, String path, PropertyType type) {
    return new Builder(name, path, type);
  }

  /**
   * Builder for {@link BrowseEntityProperty}
   */
  public static class Builder {
    private String name;
    private String path;
    private PropertyType type;

    public Builder(String name, String path, PropertyType type) {
      this.name = name;
      this.type = type;
      this.path = path;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setType(PropertyType type) {
      this.type = type;
      return this;
    }

    public BrowseEntityProperty build() {
      return new BrowseEntityProperty(name, path, type);
    }
  }
}
