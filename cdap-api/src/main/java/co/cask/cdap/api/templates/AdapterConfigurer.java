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
public interface AdapterConfigurer {

  /**
   * Set the schedule for the program. Must be set for Workflows and is not valid for other program types.
   *
   * @param schedule {@link Schedule}
   */
  public void setSchedule(Schedule schedule);

  /**
   * Set the number of instances of the program. Valid only for Workers and defaults to 1.
   *
   * @param instances number of instances
   */
  public void setInstances(int instances);

  /**
   * Set the resources the program should use.
   *
   * @param resources
   */
  public void setResources(Resources resources);

  /**
   * Add arguments to be passed to the program as runtime arguments.
   *
   * @param arguments runtime arguments
   */
  public void addRuntimeArguments(Map<String, String> arguments);

  /**
   * Add argument to be passed to the program as runtime arguments.
   *
   * @param key key
   * @param value value
   */
  public void addRuntimeArgument(String key, String value);

  /**
   * Adds a {@link Stream} to the Adapter.
   *
   * @param stream The {@link Stream} to include in the Adapter
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link DatasetModule} to be deployed during adapter creation.
   *
   * @param moduleName Name of the module to deploy
   * @param moduleClass Class of the module
   */
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in the CDAP instance) during adapter
   * creation, using {@link Dataset} as a base for the {@link DatasetModule}.
   * The module will have a single dataset type identical to the name of the class in the datasetClass parameter.
   *
   * @param datasetClass Class of the dataset; module name will be the same as the class in the parameter
   */
  void addDatasetType(Class<? extends Dataset> datasetClass);

  /**
   * Adds a Dataset instance, created automatically if absent in the CDAP instance.
   * See {@link co.cask.cdap.api.dataset.DatasetDefinition} for details.
   *
   * @param datasetName Name of the dataset instance
   * @param typeName Name of the dataset type
   * @param properties Dataset instance properties
   */
  void createDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a Dataset instance, created automatically (if absent in the CDAP instance), deploying a Dataset type
   * using the datasetClass parameter as the dataset class and the given properties.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  void createDataset(String datasetName,
                     Class<? extends Dataset> datasetClass,
                     DatasetProperties props);
}
