/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.cdap.metrics.process;

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes one second resolution v2 metrics table
 */
public class MetricsTableDeleter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableDeleter.class);

  private final DatasetFramework datasetFramework;
  private final DatasetId datasetId;

  /**
   * Schedule deletion of old metrics tables whose data have been migrated already
   */
  public MetricsTableDeleter(DatasetFramework datasetFramework, DatasetId datasetId) {
    this.datasetFramework = datasetFramework;
    this.datasetId = datasetId;
  }

  @Override
  public void run() {
    try {
      Boolean hasInstance = MigrationTableHelper.hasInstanceWithRetry(datasetFramework, datasetId);
      if (hasInstance != null && hasInstance) {
        MigrationTableHelper.deleteInstanceWithRetry(datasetFramework, datasetId);
        LOG.info("Successfully deleted the table {}", datasetId.getDataset());
      }
    } catch (Exception e) {
      LOG.error("Exception while Deleting dataset {}", datasetId.getDataset(), e);
      return;
    }
  }

}
