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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Map;

/**
 * Interface for configuring {@link Worker}.
 */
public interface WorkerConfigurer {

  /**
   * Sets the name of the {@link Worker}.
   */
  void setName(String name);

  /**
   * Set description of the {@link Worker}.
   * @param description the description
   */
  void setDescription(String description);

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
   * Sets a set of properties that will be available through the {@link WorkerSpecification#getProperties()} at runtime.
   * @param properties the properties to set
   */
  void setProperties(Map<String, String> properties);

  /**
   * Adds the names of {@link Dataset Datasets} used by the worker.
   * @param datasets dataset names
   */
  void useDatasets(Iterable<String> datasets);
}
