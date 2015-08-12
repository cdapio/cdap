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

package co.cask.cdap.api;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;

/**
 * Provides the ability to add stream, datasets.
 */
public interface DatasetConfigurer {

  /**
   * Adds a {@link Stream}.
   *
   * @param stream {@link Stream}
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link Stream} given the name of the stream.
   *
   * @param streamName name of the stream
   */
  void addStream(String streamName);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in the CDAP namespace) during application
   * deployment.
   *
   * @param moduleName Name of the module to deploy
   * @param moduleClass Class of the module
   */
  @Beta
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in the CDAP namespace) during application
   * deployment, using {@link Dataset} as a base for the {@link DatasetModule}.
   * The module will have a single dataset type identical to the name of the class in the datasetClass parameter.
   *
   * @param datasetClass Class of the dataset; module name will be the same as the class in the parameter
   */
  @Beta
  void addDatasetType(Class<? extends Dataset> datasetClass);

  /**
   * Adds a Dataset instance, created automatically if absent in the CDAP namespace.
   * See {@link co.cask.cdap.api.dataset.DatasetDefinition} for details.
   *
   * @param datasetName name of the dataset instance
   * @param typeName name of the dataset type
   * @param properties dataset instance properties
   */
  @Beta
  void createDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a Dataset instance with {@link DatasetProperties#EMPTY} created automatically if absent in the CDAP namespace.
   *
   * @param datasetName name of the dataset instance
   * @param typeName name of the dataset type
   */
  void createDataset(String datasetName, String typeName);

  /**
   * Adds a Dataset instance, created automatically (if absent in the CDAP namespace), deploying a Dataset type
   * using the datasetClass parameter as the dataset class and the given properties.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props);

  /**
   * Adds a Dataset instance with {@link DatasetProperties#EMPTY} create automatically (if absent in the
   * CDAP namespace), deploying a Dataset type using the datasetClass parameter as the dataset class.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   */
  void createDataset(String datasetName, Class<? extends Dataset> datasetClass);
}
