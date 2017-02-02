/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import javax.annotation.Nullable;

/**
 * Dataset instance metadata.
 */
public class DatasetMeta {
  private final DatasetSpecification spec;

  // todo: meta of modules inside will have list of all types in the module that is redundant here
  private final DatasetTypeMeta type;

  @SerializedName("hive_table")
  private final String hiveTableName;

  @SerializedName("principal")
  private final String ownerPrincipal;

  public DatasetMeta(DatasetSpecification spec, DatasetTypeMeta type, @Nullable String hiveTableName) {
    this(spec, type, hiveTableName, null);
  }

  public DatasetMeta(DatasetSpecification spec, DatasetTypeMeta type, @Nullable String hiveTableName,
                     @Nullable String ownerPrincipal) {
    this.spec = spec;
    this.type = type;
    this.hiveTableName = hiveTableName;
    this.ownerPrincipal = ownerPrincipal;
  }

  public DatasetSpecification getSpec() {
    return spec;
  }

  public DatasetTypeMeta getType() {
    return type;
  }

  @Nullable
  public String getHiveTableName() {
    return hiveTableName;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }
}
