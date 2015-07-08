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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * An abstract implementation of {@link HttpServiceHandler}. Classes that extend this class only
 * have to implement a configure method which can be used to add optional arguments.
 */
public abstract class AbstractHttpServiceHandler implements HttpServiceHandler {
  private HttpServiceConfigurer configurer;
  private HttpServiceContext context;

  /**
   * This can be overridden in child classes to add custom user properties during configure time.
   */
  protected void configure() {
    // no-op
  }

  /**
   * An implementation of {@link HttpServiceHandler#configure(HttpServiceConfigurer)}. Stores the configurer
   * so that it can be used later and then runs the configure method which is overwritten by children classes.
   *
   * @param configurer the {@link HttpServiceConfigurer} which is used to configure this Handler
   */
  @Override
  public final void configure(HttpServiceConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * @see HttpServiceConfigurer#addStream(Stream)
   */
  protected final void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  /**
   * @see HttpServiceConfigurer#addDatasetModule(String, Class)
   */
  @Beta
  protected final void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see HttpServiceConfigurer#addDatasetType(Class)
   */
  @Beta
  protected final void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  /**
   * Calls {@link HttpServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing empty properties.
   *
   * @see HttpServiceConfigurer#createDataset(String, String, DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Calls {@link HttpServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing the type name and
   * properties.
   *
   * @see HttpServiceConfigurer#createDataset(String, String, co.cask.cdap.api.dataset.DatasetProperties)
   */
  @Beta
  protected final void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  /**
   * Calls {@link HttpServiceConfigurer#createDataset(String, String, DatasetProperties)}, passing the dataset class
   * and properties.
   *
   * @see HttpServiceConfigurer#createDataset(String, Class, co.cask.cdap.api.dataset.DatasetProperties)
   */
  protected final void createDataset(String datasetName,
                                     Class<? extends Dataset> datasetClass,
                                     DatasetProperties properties) {
    configurer.createDataset(datasetName, datasetClass, properties);
  }

  /**
   * Calls {@link HttpServiceConfigurer#createDataset(String, Class, DatasetProperties)}, passing empty properties.
   *
   * @see HttpServiceConfigurer#createDataset(String, Class, DatasetProperties)
   */
  protected final void createDataset(String datasetName,
                                     Class<? extends Dataset> datasetClass) {
    configurer.createDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }

  /**
   * An implementation of {@link HttpServiceHandler#initialize(HttpServiceContext)}. Stores the context
   * so that it can be used later.
   *
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    this.context = context;
  }

  /**
   * An implementation of {@link HttpServiceHandler#destroy()} which does nothing
   */
  @Override
  public void destroy() {
    // no-op
  }

  /**
   * @return the {@link HttpServiceContext} which was used when this class was initialized
   */
  protected final HttpServiceContext getContext() {
    return context;
  }

  /**
   * @return the {@link HttpServiceConfigurer} used to configure this class
   */
  protected final HttpServiceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @see HttpServiceConfigurer#setProperties(java.util.Map)
   *
   * @param properties the properties to set
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the Service.
   *
   * @param dataset Dataset name.
   * @param datasets More Dataset names.
   */
  protected void useDatasets(String dataset, String...datasets) {
    List<String> datasetList = new ArrayList<>();
    datasetList.add(dataset);
    datasetList.addAll(Arrays.asList(datasets));
    useDatasets(datasetList);
  }

  /**
   * Adds the names of {@link co.cask.cdap.api.dataset.Dataset DataSets} used by the Service.
   *
   * @param datasets Dataset names.
   */
  protected void useDatasets(Iterable<String> datasets) {
    configurer.useDatasets(datasets);
  }
}
