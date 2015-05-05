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
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

/**
 *
 */
public interface TestManager {

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application
   * must be in the same or children package as the application.
   *
   * @param namespace The namespace to deploy to
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(Id.Namespace namespace,
                                       Class<? extends Application> applicationClz, File... bundleEmbeddedJars);

  /**
   * Deploys an {@link ApplicationTemplate}. Only supported in unit tests.
   *
   * @param namespace The namespace to deploy to
   * @param templateId The id of the template. Must match the name set in
   *                   {@link ApplicationTemplate#configure(ApplicationConfigurer, ApplicationContext)}
   * @param templateClz The template class
   * @param exportPackages The list of packages that should be visible to template plugins. For example,
   *                       if your plugins implement an interface com.company.api.myinterface that is in your template,
   *                       you will want to include 'com.company.api' in the list of export pacakges.
   */
  void deployTemplate(Id.Namespace namespace, Id.ApplicationTemplate templateId,
                      Class<? extends ApplicationTemplate> templateClz,
                      String... exportPackages) throws IOException;

  /**
   * Adds a plugins jar usable by the given template. Only supported in unit tests. Plugins added will not be visible
   * until a call to {@link #deployTemplate(Id.Namespace, Id.ApplicationTemplate, Class, String...)} is made. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param templateId The id of the template to add the plugin for
   * @param jarName The name to use for the plugin jar
   * @param pluginClz A plugin class to add to the jar
   * @param classes Additional plugin classes to add to the jar
   * @throws IOException
   */
  void addTemplatePlugins(Id.ApplicationTemplate templateId, String jarName, Class<?> pluginClz,
                          Class<?>... classes) throws IOException;

  /**
   * Add a template plugin configuration file.
   *
   * @param templateId the id of the template to add the plugin config for
   * @param fileName the name of the config file. The name should match the plugin jar file that it is for. For example,
   *                 if you added a plugin named hsql-jdbc-1.0.0.jar, the config file must be named hsql-jdbc-1.0.0.json
   * @param type the type of plugin
   * @param name the name of the plugin
   * @param description the description for the plugin
   * @param className the class name of the plugin
   * @param fields the fields the plugin uses
   * @throws IOException
   */
  void addTemplatePluginJson(Id.ApplicationTemplate templateId, String fileName, String type, String name,
                             String description, String className,
                             PluginPropertyField... fields) throws IOException;

  /**
   * Creates an adapter.
   *
   * @param adapterId The id of the adapter to create
   * @param config The configuration for the adapter
   * @return An {@link AdapterManager} to manage the deployed adapter.
   * @throws Exception if there was an exception deploying the adapter.
   */
  AdapterManager createAdapter(Id.Adapter adapterId, AdapterConfig config) throws Exception;

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

  /**
   * Creates a namespace.
   *
   * @param namespaceMeta the namespace to create
   * @throws Exception
   */
  void createNamespace(NamespaceMeta namespaceMeta) throws Exception;

  /**
   * Deletes a namespace.
   *
   * @param namespace the namespace to delete
   * @throws Exception
   */
  void deleteNamespace(Id.Namespace namespace) throws Exception;


  StreamManager getStreamManager(Id.Stream streamId);
}
