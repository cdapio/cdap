package com.continuuity.api.app;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.module.DatasetModule;

/**
 * Configures Reactor application.
 */
@Beta
public interface ApplicationConfigurer {
  /**
   * Sets the Application's name.
   *
   * @param name Name of the Application.
   */
  void setName(String name);

  /**
   * Sets the Application's description.
   *
   * @param description Description of the Application.
   */
  void setDescription(String description);

  /**
   * Adds a {@link Stream} to the Application.
   *
   * @param stream The {@link Stream} to be included in the Application.
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link DataSet} to the Application.
   * @param dataset The {@link DataSet} to be included in the Application.
   */
  @Deprecated
  void addDataSet(DataSet dataset);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in a system) during application deploy.
   * @param moduleName name of the module to deploy
   * @param moduleClass class of the module
   */
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a dataset instance to be created automatically (if not exists) by application components.
   * See {@link com.continuuity.internal.data.dataset.DatasetDefinition} for more details.
   * @param datasetInstanceName name of the dataset instance
   * @param typeName name of the dataset type
   * @param properties dataset instance properties
   */
  void createDataSet(String datasetInstanceName, String typeName, DatasetInstanceProperties properties);

  /**
   * Adds a {@link Flow} to the Application.
   * @param flow The {@link Flow} to be included in the Application.
   */
  void addFlow(Flow flow);

  /**
   * Adds a {@link com.continuuity.api.procedure.Procedure} to the application with one instance.
   *
   * @param procedure The {@link com.continuuity.api.procedure.Procedure} to include in the application.
   */
  void addProcedure(Procedure procedure);

  /**
   * Adds a {@link Procedure} to the application with the number of instances.
   *
   * @param procedure The {@link Procedure} to include in the application.
   * @param instances number of instances.
   */
  void addProcedure(Procedure procedure, int instances);

  /**
   * Adds MapReduce job to the application. Use it when you need to re-use existing MapReduce jobs that rely on
   * Hadoop MapReduce APIs.
   * @param mapReduce The MapReduce job to add
   */
  void addMapReduce(MapReduce mapReduce);

  /**
   * Adds a {@link Workflow} to the Application.
   * @param workflow The {@link Workflow} to be included in the Application.
   */
  void addWorkflow(Workflow workflow);
}
