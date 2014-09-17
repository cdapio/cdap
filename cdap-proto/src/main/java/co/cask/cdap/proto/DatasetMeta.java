/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetSpecification;
import com.google.gson.annotations.SerializedName;

/**
 * Dataset instance metadata.
 */
public class DatasetMeta {
  private final DatasetSpecification spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta type;

  @SerializedName("hive_table")
  private final String hiveTableName;

  public DatasetMeta(DatasetSpecification spec, DatasetTypeMeta type, String hiveTableName) {
    this.spec = spec;
    this.type = type;
    this.hiveTableName = hiveTableName;
  }

  public DatasetSpecification getSpec() {
    return spec;
  }

  public DatasetTypeMeta getType() {
    return type;
  }

  public String getHiveTableName() {
    return hiveTableName;
  }
}
