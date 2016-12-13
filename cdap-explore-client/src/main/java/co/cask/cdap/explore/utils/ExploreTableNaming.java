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

package co.cask.cdap.explore.utils;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;

/**
 * Specifies how to name tables for Explore.
 */
public final class ExploreTableNaming {

  public String getTableName(StreamId streamId) {
    return String.format("stream_%s", cleanTableName(streamId.getStream()));
  }

  public String getTableName(DatasetId datasetID) {
    return String.format("dataset_%s", cleanTableName(datasetID.getDataset()));
  }

  public String getTableName(StreamViewId viewId) {
    return String.format("stream_%s_%s", cleanTableName(viewId.getStream()), cleanTableName(viewId.getView()));
  }

  public String cleanTableName(String name) {
    // Instance name is like cdap.user.my_table.
    // For now replace . with _ and - with _ since Hive tables cannot have . or _ in them.
    return name.replaceAll("\\.", "_").replaceAll("-", "_").toLowerCase();
  }

}
