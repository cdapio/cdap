/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.DatasetConfigurer;
import co.cask.cdap.api.ProgramConfigurer;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.plugin.PluginConfigurer;

/**
 * Interface for configuring {@link Worker}.
 */
public interface WorkerConfigurer extends DatasetConfigurer, ProgramConfigurer, PluginConfigurer {

  /**
   * Sets the resources requirements for the the {@link Worker}.
   * @param resources the requirements
   */
  void setResources(Resources resources);

  /**
   * Sets the number of instances needed for the {@link Worker}.
   * @param instances number of instances, must be > 0
   */
  void setInstances(int instances);

  /**
   * Adds the names of {@link Dataset Datasets} used by the worker.
   * @param datasets dataset names
   * @deprecated Deprecated as of 2.8.0. Dataset can be requested directly through the method
   *             {@link DatasetContext#getDataset(String)} at runtime when
   *             calling {@link WorkerContext#execute(TxRunnable)}.
   */
  @Deprecated
  void useDatasets(Iterable<String> datasets);
}
