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

package co.cask.cdap.api.service;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.worker.WorkerConfigurer;

import java.util.Map;

/**
 * Interface for configuring {@link ServiceWorker} usage in a {@link Service}.
 *
 * @deprecated As of version 2.8.0, replaced by {@link WorkerConfigurer}
 */
@Deprecated
public interface ServiceWorkerConfigurer {

  /**
   * Set description of the {@link ServiceWorker}.
   * @param description the description
   */
  void setDescription(String description);

  /**
   * Sets the resources requirements for the {@link ServiceWorker}.
   * @param resources The requirements.
   */
  void setResources(Resources resources);

  /**
   * Sets the number of instances needed for the {@link ServiceWorker}.
   * @param instances Number of instances, must be > 0.
   */
  void setInstances(int instances);

  /**
   * Sets a set of properties that will be available through the {@link ServiceWorkerSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);

  /**
   * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the worker.
   *
   * @param datasets Dataset names.
   */
  void useDatasets(Iterable<String> datasets);
}
