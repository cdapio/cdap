/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.spi.provisioner;

import java.util.Objects;

/**
 * Property of a Provisioner. Includes optional restrictions on the property value and range.
 *
 * @param <T> type of property
 */
public class ProvisionerProperty<T> {
  protected final String name;
  protected final String label;
  protected final String description;
  protected final String type;

  public ProvisionerProperty(String name, String label, String description, String type) {
    this.name = name;
    this.label = label;
    this.description = description;
    this.type = type;
  }

  /**
   * Check if the specified value is valid.
   *
   * @param value the value to check
   * @throws IllegalArgumentException if the value is invalid
   */
  public void validate(T value) {
    // no-op
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProvisionerProperty that = (ProvisionerProperty) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(label, that.label) &&
      Objects.equals(description, that.description) &&
      Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, label, description, type);
  }
}
