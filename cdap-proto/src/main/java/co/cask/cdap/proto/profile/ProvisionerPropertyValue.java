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

package co.cask.cdap.proto.profile;

import java.util.Objects;

/**
 * A brief summary of provisioner property.
 */
public class ProvisionerPropertyValue {
  private final String name;
  private final String value;
  // this variable indicates whether this property is editable or not.
  private final boolean isEditable;

  public ProvisionerPropertyValue(String name, String value, boolean isEditable) {
    this.name = name;
    this.value = value;
    this.isEditable = isEditable;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public boolean isEditable() {
    return isEditable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProvisionerPropertyValue that = (ProvisionerPropertyValue) o;

    return Objects.equals(isEditable, that.isEditable) &&
      Objects.equals(name, that.name) &&
      Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value, isEditable);
  }
}
