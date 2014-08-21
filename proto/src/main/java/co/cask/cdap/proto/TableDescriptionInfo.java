/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.proto;

import com.google.common.base.Objects;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Schema and other information about a Hive table.
 */
public class TableDescriptionInfo {
  private final Map<String, String> schema;

  @SerializedName("from_dataset")
  private final boolean isBackedByDataset;

  public TableDescriptionInfo(Map<String, String> schema, boolean isBackedByDataset) {
    this.schema = schema;
    this.isBackedByDataset = isBackedByDataset;
  }

  public boolean isBackedByDataset() {
    return isBackedByDataset;
  }

  public Map<String, String> getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableDescriptionInfo that = (TableDescriptionInfo) o;

    return Objects.equal(this.schema, that.schema)
      && Objects.equal(this.isBackedByDataset, that.isBackedByDataset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, isBackedByDataset);
  }

}
