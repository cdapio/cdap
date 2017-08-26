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

import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.proto.id.DatasetId;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * To perform instance checks, get dataset, delete dataset with retry - used by metrics table migration
 */
final class MigrationTableHelper {

  private static volatile boolean stopping = false;

  private MigrationTableHelper() {
    //no-op
  }

  static void requestStop(boolean value) {
    stopping = value;
  }

  /**
   * perform getDataset datasetId with retry, we retry on ServiceUnavailable exception,
   * returns once dataset is available
   * sleep in-between
   * @return MetricsTable
   * @throws Exception; we throw interrupted exception when we are stopping
   */
  @Nullable
  static MetricsTable getDatasetWithRetry(final DatasetFramework datasetFramework,
                                          final DatasetId datasetId) throws Exception {
    return Retries.callWithRetries(new Retries.Callable<MetricsTable, Exception>() {
      @Override
      public MetricsTable call() throws Exception {
        if (!stopping) {
          return datasetFramework.getDataset(datasetId, Collections.<String, String>emptyMap(), null);
        } else {
          throw new InterruptedException(
            String.format("Giving up get dataset retry for %s, as we are stopping", datasetId.getDataset()));
        }
      }
    }, RetryStrategies.fixDelay(1, TimeUnit.SECONDS));
  }


  /**
   * perform hasInstance check for datasetId with retry, we retry on ServiceUnavailable Exception with a
   * sleep in-between
   * @return hasInstance or not
   * @throws Exception; we throw interrupted exception when we are stopping
   */
  @Nullable
  static boolean hasInstanceWithRetry(final DatasetFramework datasetFramework,
                                      final DatasetId datasetId) throws Exception {
    return Retries.callWithRetries(new Retries.Callable<Boolean, Exception>() {
      @Override
      public Boolean call() throws Exception {
        if (!stopping) {
          return datasetFramework.hasInstance(datasetId);
        } else {
          throw new InterruptedException(
            String.format("Giving up hasInstance dataset retry for %s, as we are stopping", datasetId.getDataset()));
        }
      }
    }, RetryStrategies.fixDelay(1, TimeUnit.SECONDS));
  }

  /**
   * delete dataset instance with retry if dataset service is not available. returns true if deletion is successful
   * @throws Exception; we throw interrupted exception when we are stopping
   */
  static Void deleteInstanceWithRetry(final DatasetFramework datasetFramework,
                                      final DatasetId datasetId) throws Exception {
    return Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
      @Override
      public Void call() throws Exception {
        if (!stopping) {
          datasetFramework.deleteInstance(datasetId);
          return null;
        } else {
          throw new InterruptedException(
            String.format("Giving up delete dataset retry for %s, as we are stopping", datasetId.getDataset()));
        }
      }
    }, RetryStrategies.fixDelay(1, TimeUnit.SECONDS));
  }

}
