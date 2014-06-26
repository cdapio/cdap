/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.app;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.workflow.Workflow;
import org.apache.twill.api.TwillApplication;

/**
 * A support class for {@link Application}s which reduces repetition and results in
 * a more readable configuration. Simply implement {@link #configure()} to define your application.
 */
 
 
@Beta
public abstract class AbstractApplication implements Application {
  private ApplicationContext context;
  private ApplicationConfigurer configurer;

  /**
   * Override this method to configure the application.
   */
  public abstract void configure();

  @Override
  public final void configure(ApplicationConfigurer configurer, ApplicationContext context) {
    this.context = context;
    this.configurer = configurer;

    configure();
  }

  /**
   * @return {@link ApplicationConfigurer} used to configure this {@link Application}
   */
  protected ApplicationConfigurer getConfigurer() {
    return configurer;
  }

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
   * @see ApplicationConfigurer#setDescription(String) (String)
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
   * @see ApplicationConfigurer#addDataSet(DataSet)
   */
  @Deprecated
  protected void addDataSet(DataSet dataSet) {
    configurer.addDataSet(dataSet);
  }

  /**
   * @see ApplicationConfigurer#addDatasetModule(String, Class)
   */
  protected void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  /**
   * @see ApplicationConfigurer#addDatasetType(Class)
   */
  protected void addDatasetType(Class<? extends Dataset> datasetClass) {
    configurer.addDatasetType(datasetClass);
  }

  /**
   * Calls {@link #createDataset(String, String, DatasetProperties)} and passes empty properties.
   */
  protected void createDataset(String datasetName, String typeName) {
    configurer.createDataset(datasetName, typeName, DatasetProperties.EMPTY);
  }

  /**
   * @see ApplicationConfigurer#createDataset(String, String, com.continuuity.api.dataset.DatasetProperties)
   */
  protected void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    configurer.createDataset(datasetName, typeName, properties);
  }

  /**
   * @see ApplicationConfigurer#createDataset(String, Class, com.continuuity.api.dataset.DatasetProperties)
   */
  protected void createDataset(String datasetName,
                               Class<? extends Dataset> datasetClass,
                               DatasetProperties props) {
    configurer.createDataset(datasetName, datasetClass, props);
  }

  /**
   * @see ApplicationConfigurer#createDataset(String, Class, DatasetProperties) and passes epty properties
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
   * @see ApplicationConfigurer#addService(TwillApplication)
   */
  protected void addService(TwillApplication application) {
    configurer.addService(application);
  }
}
