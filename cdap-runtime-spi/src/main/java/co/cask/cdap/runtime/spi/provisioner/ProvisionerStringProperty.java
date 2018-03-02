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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A String property. Includes optional restrictions on the property value and range.
 */
public class ProvisionerStringProperty extends ProvisionerProperty<String> {
  private final Set<String> values;

  public ProvisionerStringProperty(String name, String label, String description, @Nullable Set<String> values) {
    super(name, label, description, "string");
    this.values = Collections.unmodifiableSet(values);
  }

  @Nullable
  public Set<String> getValues() {
    return values;
  }

  @Override
  public void validate(String value) {
    if (values != null && !values.contains(value)) {
      throw new IllegalArgumentException(String.format("Invalid value '%s' = '%s'. Must be one of %s.",
                                                       name, value, values.stream().collect(Collectors.joining(", "))));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ProvisionerStringProperty that = (ProvisionerStringProperty) o;

    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), values);
  }
}
