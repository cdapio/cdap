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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;

/**
 * Configurer for a pipeline, that delegates all operations to an AdapterConfigurer.
 */
public class DefaultPipelineConfigurer implements PipelineConfigurer {
  private final AdapterConfigurer adapterConfigurer;

  public DefaultPipelineConfigurer(AdapterConfigurer adapterConfigurer) {
    this.adapterConfigurer = adapterConfigurer;
  }

  @Override
  public void addStream(Stream stream) {
    adapterConfigurer.addStream(stream);
  }

  @Override
  public void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    adapterConfigurer.addDatasetModule(moduleName, moduleClass);
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> datasetClass) {
    adapterConfigurer.addDatasetType(datasetClass);
  }

  @Override
  public void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    adapterConfigurer.createDataset(datasetName, typeName, properties);
  }

  @Override
  public void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    adapterConfigurer.createDataset(datasetName, datasetClass, props);
  }
}
