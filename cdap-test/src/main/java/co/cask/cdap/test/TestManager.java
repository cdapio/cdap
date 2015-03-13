/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.proto.Id;

import java.io.File;
import java.sql.Connection;

/**
 *
 */
public interface TestManager {

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * {@link co.cask.cdap.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param namespace The namespace to deploy to
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(Id.Namespace namespace,
                                       Class<? extends Application> applicationClz, File... bundleEmbeddedJars);

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   */
  void clear() throws Exception;

  /**
   * Deploys {@link DatasetModule}.
   *
   * @param namespace namespace to deploy the dataset module to
   * @param moduleName name of the module
   * @param datasetModule module class
   * @throws Exception
   */
  @Beta
  void deployDatasetModule(Id.Namespace namespace, String moduleName, Class<? extends DatasetModule> datasetModule)
    throws Exception;

  /**
   * Adds an instance of a dataset.
   *
   * @param namespace namespace of the dataset
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   */
  @Beta
  <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                String datasetTypeName, String datasetInstanceName,
                                                DatasetProperties props) throws Exception;

  /**
   * Adds an instance of dataset.
   *
   * @param namespace namespace of the dataset
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param <T> type of the dataset admin
   */
  @Beta
  <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                String datasetTypeName, String datasetInstanceName) throws Exception;

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   *
   * @param namespace namespace of the dataset
   * @param datasetInstanceName instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  @Beta
  <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception;

  /**
   * @param namespace namespace to interact within
   * @return a JDBC connection that allows to run SQL queries over data sets.
   */
  @Beta
  Connection getQueryClient(Id.Namespace namespace) throws Exception;
}
