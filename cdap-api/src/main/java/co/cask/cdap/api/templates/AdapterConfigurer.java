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

package co.cask.cdap.api.templates;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.schedule.Schedule;

import java.util.Map;

/**
 * Configurer used to configure program used in the execution of the Adapter.
 * Currently, only Worker or Workflow can be used in the execution.
 */
@Beta
public interface AdapterConfigurer extends AdapterPluginRegistry {

  /**
   * Set the schedule for the program. Must be set for Workflows and is not valid for other program types.
   *
   * @param schedule {@link Schedule}
   */
  void setSchedule(Schedule schedule);

  /**
   * Set the number of instances of the program. Valid only for Workers and defaults to 1.
   *
   * @param instances number of instances
   */
  void setInstances(int instances);

  /**
   * Set the resources the program should use.
   *
   * @param resources the resources the program should use
   */
  void setResources(Resources resources);

  /**
   * Add arguments to be passed to the program as runtime arguments.
   *
   * @param arguments the runtime arguments to add to the program
   */
  void addRuntimeArguments(Map<String, String> arguments);

  /**
   * Add argument to be passed to the program as runtime arguments.
   *
   * @param key the runtime argument key
   * @param value the runtime argument value
   */
  void addRuntimeArgument(String key, String value);

  /**
   * Adds a {@link Stream} to the Adapter. The stream will be created during adapter creation if it does not
   * already exist.
   *
   * @param stream the {@link Stream} to add in the Adapter
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link DatasetModule} to be deployed during adapter creation if it does not already exist.
   *
   * @param moduleName the name of the module to deploy
   * @param moduleClass the class of the module
   */
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically during adapter creation if it does not already exist.
   * Uses {@link Dataset} as a base for the {@link DatasetModule}.
   * The module will have a single dataset type identical to the name of the class in the datasetClass parameter.
   *
   * @param datasetClass the class of the dataset, with the module name the same as the class in the parameter
   */
  void addDatasetType(Class<? extends Dataset> datasetClass);

  /**
   * Adds a Dataset instance to the Adapter. The Dataset instance will be created during adapter creation if it
   * does not already exist.
   *
   * @param datasetName the name of the dataset instance
   * @param typeName the name of the dataset type
   * @param properties the properties of the dataset instance
   */
  void createDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a Dataset instance to the Adapter. The Dataset instance will be created during adapter creation if it
   * does not already exist.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props);
}
