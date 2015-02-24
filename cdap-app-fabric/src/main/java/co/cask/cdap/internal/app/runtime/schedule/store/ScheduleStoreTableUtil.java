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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.lib.table.MetaTableUtil;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Helper class for working with the dataset table used by
 * {@link DatasetBasedTimeScheduleStore}.
 */
public class ScheduleStoreTableUtil extends MetaTableUtil {
  public static final String SCHEDULE_STORE_DATASET_NAME = "schedulestore";

  @Inject
  public ScheduleStoreTableUtil(DatasetFramework framework, CConfiguration conf) {
    super(framework, conf);
  }

  @Override
  public String getMetaTableName() {
    return SCHEDULE_STORE_DATASET_NAME;
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by schedule mds.
   * @param datasetFramework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework datasetFramework) throws IOException, DatasetManagementException {
    Id.DatasetInstance scheduleStoreDatasetInstance =
      Id.DatasetInstance.from(SYSTEM_NAMESPACE, SCHEDULE_STORE_DATASET_NAME);
    datasetFramework.addInstance(Table.class.getName(), scheduleStoreDatasetInstance, DatasetProperties.EMPTY);
  }
}
