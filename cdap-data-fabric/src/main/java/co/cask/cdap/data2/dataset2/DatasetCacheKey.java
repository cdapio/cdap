/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import com.google.common.base.Objects;

import java.util.Map;

/**
 * A key used by implementations of {@link DynamicDatasetContext} to cache Datasets. Includes the dataset name
 * and it's arguments.
 */
public final class DatasetCacheKey {
  private final String name;
  private final Map<String, String> arguments;

  public DatasetCacheKey(String name, Map<String, String> arguments) {
    this.name = name;
    this.arguments = arguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatasetCacheKey that = (DatasetCacheKey) o;

    return Objects.equal(this.name, that.name) &&
      Objects.equal(this.arguments, that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, arguments);
  }
}
