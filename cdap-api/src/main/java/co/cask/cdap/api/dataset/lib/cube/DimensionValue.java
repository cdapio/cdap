/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;

import javax.annotation.Nullable;

/**
 * Represents dimension and its value associated with {@link CubeFact}.
 */
@Beta
public final class DimensionValue {
  private final String name;
  private final String value;

  public DimensionValue(String name, @Nullable String value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  @Nullable
  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DimensionValue dimensionValue = (DimensionValue) o;

    boolean result = value == null ? dimensionValue.value == null : value.equals(dimensionValue.value);
    return result && name.equals(dimensionValue.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode() + (value == null ? 0 : 31 * value.hashCode());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DimensionValue");
    sb.append("{name='").append(name).append('\'');
    sb.append(", value='").append(value == null ? "null" : value).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
