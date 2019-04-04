/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.explore.utils;

import io.cdap.cdap.api.dataset.ExploreProperties;
import io.cdap.cdap.proto.id.DatasetId;

import java.util.Map;

/**
 * Specifies how to name tables for Explore.
 */
public final class ExploreTableNaming {

  public String getTableName(DatasetId datasetID) {
    return getTableName(datasetID, null);
  }

  public String getTableName(DatasetId datasetID, Map<String, String> properties) {
    if (properties != null) {
      String tableName = ExploreProperties.getExploreTableName(properties);
      if (tableName != null) {
        return tableName;
      }
    }
    return String.format("dataset_%s", cleanTableName(datasetID.getDataset()));
  }

  public String cleanTableName(String name) {
    // Instance name is like cdap.user.my_table.
    // For now replace . with _ and - with _ since Hive tables cannot have . or _ in them.
    return name.replaceAll("\\.", "_").replaceAll("-", "_").toLowerCase();
  }

}
