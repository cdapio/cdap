/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.metadata;

import co.cask.cdap.api.annotation.Beta;

import java.util.Objects;

/**
 * Specifies the sorting of metadata search results.
 */
@Beta
public class Sorting {

  /**
   * Whether to sort in ascending or descending order.
   */
  public enum Order { ASC, DESC }

  private final String key;
  private final Order order;

  /**
   * @param key the field to sort by
   * @param order whether to sort ascending or descending
   */
  public Sorting(String key, Order order) {
    this.key = key.toLowerCase();
    this.order = order;
  }

  public String getKey() {
    return key;
  }

  public Order getOrder() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Sorting sorting = (Sorting) o;
    return Objects.equals(key, sorting.key) &&
      order == sorting.order;
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, order);
  }

  @Override
  public String toString() {
    return key + ' ' + order;
  }

  public static Sorting of(String str) {
    String[] parts = str.trim().split("\\s+");
    if (parts.length == 1 && !parts[0].isEmpty()) {
      return new Sorting(parts[0], Order.ASC);
    } else if (parts.length == 2) {
      return new Sorting(parts[0], Order.valueOf(parts[1].toUpperCase()));
    } else {
      throw new IllegalArgumentException(
        String.format("Invalid value '%s' for Sorting. It must be of the form '<fieldname> [ ASC | DESC ]'", str));
    }
  }

}
