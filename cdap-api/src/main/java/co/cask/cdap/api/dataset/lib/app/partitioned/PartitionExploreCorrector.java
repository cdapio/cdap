/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.app.partitioned;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.worker.AbstractWorker;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An application that can be used to bring the Explore state of a (Time)PartitionedFileSet in sync with
 * the actual partitions of the dataset. This can be necessary after certain failure conditions, dataset
 * property updates, or for datasets that were not enabled for explore when some of the partitions were created.
 */
public class PartitionExploreCorrector extends AbstractApplication {

  @Override
  public void configure() {
    setDescription("An app to correct the Explore state of a partitioned file set. Run the worker with " +
                     "dataset.name=<name> [ verbose=<boolean> ] [ batch.size=<int> ] [ disable.explore=<boolean> ]");
    addWorker(new PartitionWorker());
  }

  /**
   * This worker can be used to bring a partitioned file set in sync with Explore. Run the worker with runtime
   * arguments: dataset.name=<name> [ verbose=<boolean> ] [ batch.size=<int> ] [ disable.explore=<boolean> ].
   */
  public static class PartitionWorker extends AbstractWorker {
    @Override
    public void run() {
      final String datasetName = getContext().getRuntimeArguments().get("dataset.name");
      if (datasetName == null) {
        throw new IllegalArgumentException("'dataset.name' must be given as a runtime argument.");
      }
      boolean verbose = Boolean.valueOf(getContext().getRuntimeArguments().get("verbose"));
      String batchSizeArg = getContext().getRuntimeArguments().get("batch.size");
      int batchSize = batchSizeArg == null ? 50 : Integer.parseInt(batchSizeArg);
      String disableArg = getContext().getRuntimeArguments().get("disable.explore");
      boolean doDisable = disableArg == null || Boolean.valueOf(disableArg);

      final AtomicReference<Class<?>> pfsClass = new AtomicReference<>();
      getContext().execute(new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          pfsClass.set(context.getDataset(datasetName).getClass());
        }
      });
      try {
        pfsClass.get()
          .getMethod("fixPartitions", Transactional.class, String.class, boolean.class, int.class, boolean.class)
          .invoke(null, // static method call
                  getContext(), datasetName, doDisable, batchSize, verbose);
      } catch (Exception e) {
        throw new RuntimeException("Failed to call fixPartitions() using reflection", e);
      }
    }
  }
}
