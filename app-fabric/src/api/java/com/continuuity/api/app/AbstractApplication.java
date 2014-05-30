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

  protected ApplicationContext getApplicationContext() {
    return context;
  }

  protected void setName(String name) {
    configurer.setName(name);
  }

  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  protected void addStream(Stream stream) {
    configurer.addStream(stream);
  }

  @Deprecated
  protected void addDataSet(DataSet dataSet) {
    configurer.addDataSet(dataSet);
  }

  protected void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass) {
    configurer.addDatasetModule(moduleName, moduleClass);
  }

  protected void createDataSet(String datasetInstanceName, String typeName, DatasetInstanceProperties properties) {
    configurer.createDataSet(datasetInstanceName, typeName, properties);
  }

  protected void createDataSet(String datasetInstanceName, String typeName) {
    configurer.createDataSet(datasetInstanceName, typeName, DatasetInstanceProperties.EMPTY);
  }

  protected void addFlow(Flow flow) {
    configurer.addFlow(flow);
  }

  protected void addProcedure(Procedure procedure) {
    configurer.addProcedure(procedure);
  }

  protected void addProcedure(Procedure procedure, int instances) {
    configurer.addProcedure(procedure, instances);
  }

  protected void addMapReduce(MapReduce mapReduce) {
    configurer.addMapReduce(mapReduce);
  }

  protected void addWorkflow(Workflow workflow) {
    configurer.addWorkflow(workflow);
  }
}
