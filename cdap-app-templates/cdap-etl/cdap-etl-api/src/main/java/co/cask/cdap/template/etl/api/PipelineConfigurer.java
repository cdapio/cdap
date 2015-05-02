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

package co.cask.cdap.template.etl.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.templates.AdapterPluginRegistry;

/**
 * Configures an ETL Pipeline. Allows adding datasets and streams, which will be created when a pipeline is created.
 */
@Beta
public interface PipelineConfigurer extends AdapterPluginRegistry {

  /**
   * Adds a {@link Stream} to the pipeline. The stream will be created during pipeline creation if it does not
   * already exist.
   *
   * @param stream the {@link Stream} to add to the pipeline
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link DatasetModule} to be deployed during pipeline creation if it does not already exist.
   *
   * @param moduleName the name of the module to deploy
   * @param moduleClass the class of the module
   */
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically during pipeline creation if it does not already exist.
   * Uses {@link Dataset} as a base for the {@link DatasetModule}.
   * The module will have a single dataset type identical to the name of the class in the datasetClass parameter.
   *
   * @param datasetClass the class of the dataset, with the module name the same as the class in the parameter
   */
  void addDatasetType(Class<? extends Dataset> datasetClass);

  /**
   * Adds a Dataset instance to the pipeline. The Dataset instance will be created during pipeline creation if it
   * does not already exist.
   *
   * @param datasetName the name of the dataset instance
   * @param typeName the name of the dataset type
   * @param properties the properties of the dataset instance
   */
  void createDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a Dataset instance to the pipeline. The Dataset instance will be created during pipeline creation if it
   * does not already exist.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props);
}
