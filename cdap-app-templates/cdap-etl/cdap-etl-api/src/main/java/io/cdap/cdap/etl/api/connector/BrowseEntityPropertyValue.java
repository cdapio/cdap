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
public class BrowseEntityPropertyValue {

  /**
   * The property type for the browse entity property
   */
  public enum PropertyType {
    STRING,
    DATE_LOCAL_ISO,
    TIMESTAMP_MILLIS,
    NUMBER,
    SIZE_BYTES,
    SAMPLE_DEFAULT
  }


  private final String value;
  private final PropertyType type;

  private BrowseEntityPropertyValue(String value, PropertyType type) {
    this.value = value;
    this.type = type;
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

    BrowseEntityPropertyValue that = (BrowseEntityPropertyValue) o;
    return Objects.equals(value, that.value)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, type);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder(String value, PropertyType type) {
    return new Builder(value, type);
  }

  /**
   * Builder for {@link BrowseEntityPropertyValue}
   */
  public static class Builder {

    private String value;
    private PropertyType type;

    public Builder(String value, PropertyType type) {
      this.type = type;
      this.value = value;
    }

    public Builder setValue(String value) {
      this.value = value;
      return this;
    }

    public Builder setType(PropertyType type) {
      this.type = type;
      return this;
    }

    public BrowseEntityPropertyValue build() {
      // TODO: CDAP-18062 validate if the type matches the format, for example, if type is a date,
      //  validate the value is ISO format
      return new BrowseEntityPropertyValue(value, type);
    }
  }
}
