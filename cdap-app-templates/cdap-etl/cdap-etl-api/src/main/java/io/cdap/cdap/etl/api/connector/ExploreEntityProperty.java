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
import javax.annotation.Nullable;

/**
 * Object that represents an explore entity property.
 */
public class ExploreEntityProperty {
  private final String key;
  private final Object value;
  private final String type;

  public ExploreEntityProperty(String key, @Nullable Object value, String type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public String getKey() {
    return key;
  }

  @Nullable
  public Object getValue() {
    return value;
  }

  public String getType() {
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

    ExploreEntityProperty that = (ExploreEntityProperty) o;
    return Objects.equals(key, that.key) &&
      Objects.equals(value, that.value) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, type);
  }
}
