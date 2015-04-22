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

package co.cask.cdap.data2.registry;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * Utility to create {@link UsageDataset}.
 */
public final class UsageDatasets {
  private static final Id.DatasetInstance USAGE_INSTANCE_ID =
    Id.DatasetInstance.from(Constants.SYSTEM_NAMESPACE_ID, "usage.registry");

  private UsageDatasets() {
  }

  public static UsageDataset get(DatasetFramework framework) throws IOException, DatasetManagementException {
    return get(framework, USAGE_INSTANCE_ID);
  }

  @VisibleForTesting
  static UsageDataset get(DatasetFramework framework, Id.DatasetInstance id)
    throws IOException, DatasetManagementException {
    // Use ConflictDetection.NONE as we only need a flag whether a program uses a dataset/stream.
    // Having conflict detection will lead to failures when programs start, and all try to register at the same time.
    DatasetProperties properties = DatasetProperties.builder()
      .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.NONE.name())
      .build();

    Table table = DatasetsUtil.getOrCreateDataset(framework, id, Table.class.getName(), properties, null, null);
    return new UsageDataset(table);
  }
}
