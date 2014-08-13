/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.app;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.workflow.Workflow;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunnable;

/**
 * A support class for {@link Application Applications} which reduces repetition and results in
 * a more readable configuration.
 *
 * <p>
 * Implement the {@link #configure()} method to define your application.
 * </p>
 * 
 * @see co.cask.cdap.api.app
 */
public abstract class AbstractApplication implements Application {
  private ApplicationContext context;
  private ApplicationConfigurer configurer;

  /**
   * Override this method to declare and configure the application.
   */
  public abstract void configure();

  @Override
  public final void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    this.context = context;
    this.configurer = configurer;

    configure();
  }

  /**
   * @return The {@link ApplicationConfigurer} used to configure the {@link Application}
   */
  protected ApplicationConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * @return The {@link ApplicationContext} of the {@link Application}
   */
  protected final ApplicationContext getContext() {
    return context;
  }

  /**
   * @see ApplicationConfigurer#setName(String)
   */
  protected void setName(String name) {
    configurer.setName(name);
  }

  /**
   * @see ApplicationConfigurer#setDescription(String)
   */
  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * @see ApplicationConfigurer#addStream(Stream)
   */
  protected void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  /**
   * @see ApplicationConfigurer#addDatasetModule(String, Class)
   */
  @Beta
  protected void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see ApplicationConfigurer#addDatasetType(Class)
   */
  @Beta
  protected void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing empty properties.
   *
   * @see ApplicationConfigurer#createDataset(String, String, DatasetProperties)
   */
  @Beta
  protected void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing the type name and
   * properties.
   *
   * @see ApplicationConfigurer#createDataset(String, String, co.cask.cdap.api.dataset.DatasetProperties)
   */
  @Beta
  protected void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, String, DatasetProperties)}, passing the dataset class
   * and properties.
   *
   * @see ApplicationConfigurer#createDataset(String, Class, co.cask.cdap.api.dataset.DatasetProperties)
   */
  protected void createDataset(String datasetName,
                               Class<? extends Dataset> datasetClass,
                               DatasetProperties properties) {
    configurer.createDataset(datasetName, datasetClass, properties);
  }

  /**
   * Calls {@link ApplicationConfigurer#createDataset(String, Class, DatasetProperties)}, passing empty properties.
   *
   * @see ApplicationConfigurer#createDataset(String, Class, DatasetProperties)
   */
  protected void createDataset(String datasetName,
                               Class<? extends Dataset> datasetClass) {
    configurer.createDataset(datasetName, datasetClass, DatasetProperties.EMPTY);
  }

  /**
   * @see ApplicationConfigurer#addFlow(Flow)
   */
  protected void addFlow(Flow flow) {
    configurer.addFlow(flow);
  }

  /**
   * @see ApplicationConfigurer#addProcedure(Procedure)
   */
  protected void addProcedure(Procedure procedure) {
    configurer.addProcedure(procedure);
  }

  /**
   * @see ApplicationConfigurer#addProcedure(Procedure, int)
   */
  protected void addProcedure(Procedure procedure, int instances) {
    configurer.addProcedure(procedure, instances);
  }

  /**
   * @see ApplicationConfigurer#addMapReduce(MapReduce)
   */
  protected void addMapReduce(MapReduce mapReduce) {
    configurer.addMapReduce(mapReduce);
  }

  /**
   * @see ApplicationConfigurer#addWorkflow(Workflow)
   */
  protected void addWorkflow(Workflow workflow) {
    configurer.addWorkflow(workflow);
  }

  /**
   * @see ApplicationConfigurer#addService(String, Iterable<HttpServiceHandler>);
   */
  protected void addService(String name, Iterable<HttpServiceHandler> handlers) {
    configurer.addService(name, handlers);
  }

  /**
   * @see ApplicationConfigurer#addService(String, HttpServiceHandler)
   */
  protected void addService(String name, HttpServiceHandler handler) {
    configurer.addService(name, handler);
  }
}
