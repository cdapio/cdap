package com.continuuity.api.app;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * A support class for {@link Application}s which reduces repetition and results in
 * a more readable configuration. Simply implement {@link #configure()} to define your application.
 */
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
   * @see ApplicationConfigurer#createDataSet(String, String, DatasetInstanceProperties)
   */
  protected void createDataSet(String datasetInstanceName, String typeName, DatasetInstanceProperties properties) {
    configurer.createDataSet(datasetInstanceName, typeName, properties);
  }

  /**
   * @see ApplicationConfigurer#createDataSet(String, String, DatasetInstanceProperties), this one passes
   * {@link DatasetInstanceProperties#EMPTY} as properties.
   */
  protected void createDataSet(String datasetInstanceName, String typeName) {
    configurer.createDataSet(datasetInstanceName, typeName, DatasetInstanceProperties.EMPTY);
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
}
