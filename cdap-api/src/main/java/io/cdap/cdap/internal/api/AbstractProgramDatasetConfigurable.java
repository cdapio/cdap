/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.api;

import io.cdap.cdap.api.DatasetConfigurer;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;

/**
 * Helper methods to help add stream/dataset to Programs.
 *
 * @param <T> Program's configurer
 */
public abstract class AbstractProgramDatasetConfigurable<T extends DatasetConfigurer> {

  protected abstract T getConfigurer();

  /**
   * @see DatasetConfigurer#addDatasetModule(String, Class)
   */
  @Beta
  protected final void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    getConfigurer().addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see DatasetConfigurer#addDatasetType(Class)
   */
  @Beta
  protected final void addDatasetType(Class<? extends Dataset> datasetClass) {
    getConfigurer().addDatasetType(datasetClass);
  }

  /**
   * Calls {@link DatasetConfigurer#createDataset(String, String, DatasetProperties)}, passing empty properties.
   *
   * @see DatasetConfigurer#createDataset(String, String, DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName) {
    getConfigurer().createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Calls {@link DatasetConfigurer#createDataset(String, String, DatasetProperties)}, passing the type name and
   * properties.
   *
   * @see DatasetConfigurer#createDataset(String, String, io.cdap.cdap.api.dataset.DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    getConfigurer().createDataset(datasetName, typeName, properties);
  }

  /**
   * Calls {@link DatasetConfigurer#createDataset(String, String, DatasetProperties)}, passing the dataset class
   * and properties.
   *
   * @see DatasetConfigurer#createDataset(String, Class, io.cdap.cdap.api.dataset.DatasetProperties)
   */
  protected final void createDataset(String datasetName, Class<? extends Dataset> datasetClass,
                                     DatasetProperties properties) {
    getConfigurer().createDataset(datasetName, datasetClass, properties);
  }

  /**
   * Calls {@link DatasetConfigurer#createDataset(String, Class, DatasetProperties)}, passing empty properties.
   *
   * @see DatasetConfigurer#createDataset(String, Class, DatasetProperties)
   */
  protected final void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    getConfigurer().createDataset(datasetName, datasetClass);
  }

}
