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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.proto.id.DatasetId;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * To perform instance checks, get dataset, delete dataset with retry - used by metrics table migration
 */
final class MigrationTableHelper {

  private MigrationTableHelper() {
    //no-op
  }

  /**
   * perform getDataset datasetId with retry, we retry on ServiceUnavailable exception,
   * returns once dataset is available
   * sleep in-between
   * @param datasetFramework
   * @param v2MetricsTableId
   * @return
   * @throws IOException
   * @throws DatasetManagementException
   */
  static MetricsTable getDatasetWithRetry(DatasetFramework datasetFramework,
                                          DatasetId v2MetricsTableId) throws IOException, DatasetManagementException {
    MetricsTable metricsTable = null;
    while (metricsTable == null) {
      try {
        metricsTable = datasetFramework.getDataset(v2MetricsTableId, Collections.<String, String>emptyMap(), null);
      } catch (ServiceUnavailableException e) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return metricsTable;
  }

  /**
   * perform hasInstance check for datasetId with retry, we retry on ServiceUnavailable Exception with a
   * sleep in-between
   * @param datasetFramework
   * @param datasetId
   * @return
   * @throws DatasetManagementException
   */
  static boolean hasInstanceWithRetry(DatasetFramework datasetFramework,
                                      DatasetId datasetId) throws DatasetManagementException {
    while (true) {
      try {
        return datasetFramework.hasInstance(datasetId);
      } catch (ServiceUnavailableException e) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * delete dataset instance with retry if dataset service is not available.
   * @param datasetFramework
   * @param datasetId
   * @throws DatasetManagementException
   * @throws IOException
   */
  static void deleteInstanceWithRetry(DatasetFramework datasetFramework,
                                      DatasetId datasetId) throws DatasetManagementException, IOException {
    while (true) {
      try {
        datasetFramework.deleteInstance(datasetId);
        break;
      } catch (ServiceUnavailableException e) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

}
