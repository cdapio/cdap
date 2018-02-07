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
import javax.annotation.Nullable;

/**
 * A numeric property. Includes optional restrictions on the property value and range.
 *
 * @param <T> type of number
 */
public class ProvisionerNumericProperty<T extends Number & Comparable<T>> extends ProvisionerProperty<T> {
  private final Range<T> range;

  public ProvisionerNumericProperty(String name, String label, String description, String type,
                                    @Nullable Range<T> range) {
    super(name, label, description, type);
    this.range = range;
  }

  @Nullable
  public Range<T> getRange() {
    return range;
  }

  @Override
  public void validate(T value) {
    if (range != null && !range.isInRange(value)) {
      throw new IllegalArgumentException(String.format("Invalid value '%s' = '%s'. Must be in range %s.",
                                                       name, value, range));
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

    ProvisionerNumericProperty<?> that = (ProvisionerNumericProperty<?>) o;

    return Objects.equals(range, that.range);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), range);
  }
}
