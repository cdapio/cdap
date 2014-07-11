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
 * Configures a Reactor Application.
 */
public interface ApplicationConfigurer {
  /**
   * Sets the Application's name.
   *
   * @param name The Application name
   */
  void setName(String name);

  /**
   * Sets the Application's description.
   *
   * @param description The Application description
   */
  void setDescription(String description);

  /**
   * Adds a {@link Stream} to the Application.
   *
   * @param stream The {@link Stream} to include in the Application
   */
  void addStream(Stream stream);

  /**
   * Adds a {@link DataSet} to the Application.
   *
   * @param dataset The {@link DataSet} to include in the Application
   * @deprecated As of Reactor 2.3.0
   */
  @Deprecated
  void addDataSet(DataSet dataset);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in the Reactor) during application 
   * deployment.
   *
   * @param moduleName Name of the module to deploy
   * @param moduleClass Class of the module
   */
  @Beta
  void addDatasetModule(String moduleName, Class<? extends DatasetModule> moduleClass);

  /**
   * Adds a {@link DatasetModule} to be deployed automatically (if absent in the Reactor) during application
   * deployment, using {@link Dataset} as a base for the {@link DatasetModule}.
   * The module will have a single dataset type identical to the name of the class in the datasetClass parameter.
   *
   * @param datasetClass Class of the dataset; module name will be the same as the class in the parameter
   */
  @Beta
  void addDatasetType(Class<? extends Dataset> datasetClass);

  /**
   * Adds a Dataset instance, created automatically if absent in the Reactor.
   * See {@link com.continuuity.api.dataset.DatasetDefinition} for details.
   *
   * @param datasetName Name of the dataset instance
   * @param typeName Name of the dataset type
   * @param properties Dataset instance properties
   */
  @Beta
  void createDataset(String datasetName, String typeName, DatasetProperties properties);

  /**
   * Adds a Dataset instance, created automatically (if absent in the Reactor), deploying a Dataset type
   * using the datasetClass parameter as the dataset class and the given properties.
   *
   * @param datasetName dataset instance name
   * @param datasetClass dataset class to create the Dataset type from
   * @param props dataset instance properties
   */
  void createDataset(String datasetName,
                     Class<? extends Dataset> datasetClass,
                     DatasetProperties props);

  /**
   * Adds a {@link Flow} to the Application.
   *
   * @param flow The {@link Flow} to include in the Application
   */
  void addFlow(Flow flow);

  /**
   * Adds a {@link Procedure} to the Application with a single instance.
   *
   * @param procedure The {@link Procedure} to include in the Application
   */
  void addProcedure(Procedure procedure);

  /**
   * Adds a {@link Procedure} to the Application with a number of instances.
   *
   * @param procedure The {@link Procedure} to include in the Application
   * @param instances Number of instances to be included
   */
  void addProcedure(Procedure procedure, int instances);

  /**
   * Adds a {@link MapReduce MapReduce job} to the Application. Use it when you need to re-use existing MapReduce jobs
   * that rely on Hadoop MapReduce APIs.
   *
   * @param mapReduce The {@link MapReduce MapReduce job} to include in the Application
   */
  void addMapReduce(MapReduce mapReduce);

  /**
   * Adds a {@link Workflow} to the Application.
   *
   * @param workflow The {@link Workflow} to include in the Application
   */
  void addWorkflow(Workflow workflow);

  /**
   * Adds a Custom Service {@link TwillApplication} to the Application.
   *
   * @param application Custom Service {@link TwillApplication} to include in the Application
   */
  void addService(TwillApplication application);
}
