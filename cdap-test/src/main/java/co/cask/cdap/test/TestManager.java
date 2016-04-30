/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.File;
import java.sql.Connection;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Class that provides utility methods for writing tests for CDAP Applications.
 */
public interface TestManager {

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application
   * must be in the same or children package as the application.
   *
   * @param namespace The namespace to deploy to
   * @param applicationClz The application class
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(Id.Namespace namespace,
                                       Class<? extends Application> applicationClz, File... bundleEmbeddedJars);

  /**
   * Deploys an {@link Application}.
   *
   * @param namespace The namespace to deploy to
   * @param applicationClz The application class
   * @param configObject Configuration object to be used during deployment and can be accessed
   *                     in {@link Application#configure} via {@link AbstractApplication#getConfig}
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                       @Nullable Config configObject, File... bundleEmbeddedJars);

  /**
   * Deploys an {@link Application}.
   *
   * @param appId the id of the application to create
   * @param appRequest the app create or update request that includes the artifact to create the app from and any config
   *                   to pass to the application.
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(Id.Application appId, AppRequest appRequest) throws Exception;

  /**
   * Gets an Application Manager for an {@link Application}.
   *
   * @param appId the id of deployed application
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  ApplicationManager getApplicationManager(ApplicationId appId) throws Exception;

  /**
   * Adds the specified artifact.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactFile the contents of the artifact. Must be a valid jar file containing apps or plugins
   * @deprecated since 3.4.0 use {@link #addArtifact(ArtifactId, File)}
   */
  @Deprecated
  void addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception;

  /**
   * Adds the specified artifact.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactFile the contents of the artifact. Must be a valid jar file containing apps or plugins
   * @return an {@link ArtifactManager} to manage the added artifact
   */
  ArtifactManager addArtifact(ArtifactId artifactId, File artifactFile) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @deprecated since 3.4.0. Use {@link #addAppArtifact(ArtifactId, Class)}
   */
  @Deprecated
  void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @return an {@link ArtifactManager} for managing the added app artifact
   */
  ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param exportPackages the packages to export and place in the manifest of the jar to build. This should include
   *                       packages that contain classes that plugins for the application will implement.
   * @deprecated since 3.4.0. Use {@link #addAppArtifact(ArtifactId, Class, String...)} instead
   */
  @Deprecated
  void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, String... exportPackages) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param exportPackages the packages to export and place in the manifest of the jar to build. This should include
   *                       packages that contain classes that plugins for the application will implement.
   * @return an {@link ArtifactManager} to manage the added app artifact
   */
  ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                 String... exportPackages) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param manifest the manifest to use when building the jar
   * @deprecated since 3.4.0. Use {@link #addAppArtifact(ArtifactId, Class, Manifest)} instead
   */
  @Deprecated
  void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception;

  /**
   * Builds an application artifact from the specified class and then adds it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param manifest the manifest to use when building the jar
   * @return an {@link ArtifactManager} to manage the added app artifact
   */
  ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                 Manifest manifest) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @deprecated since 3.4.0. Use {@link #addPluginArtifact(ArtifactId, ArtifactId, Class, Class[])}
   */
  @Deprecated
  void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                         Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return an {@link ArtifactManager} for managing the added artifact
   */
  ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                    Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parents the parent artifacts it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @deprecated since 3.4.0. Use {@link #addPluginArtifact(ArtifactId, Set, Class, Class[])}
   */
  void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                         Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parents the parent artifacts it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return an {@link ArtifactManager} to manage the added plugin artifact
   */
  ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                    Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;


  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param additionalPlugins any plugin classes that need to be explicitly declared because they cannot be found
   *                          by inspecting the jar. This is true for 3rd party plugins, such as jdbc drivers
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @deprecated since 3.4.0.
   * Use {@link #addPluginArtifact(ArtifactId, ArtifactId, Set, Class, Class[])}
   */
  @Deprecated
  void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                         @Nullable Set<PluginClass> additionalPlugins,
                         Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parent the parent artifact it extends
   * @param additionalPlugins any plugin classes that need to be explicitly declared because they cannot be found
   *                          by inspecting the jar. This is true for 3rd party plugins, such as jdbc drivers
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return an {@link ArtifactManager} to manage this artifact
   */
  ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parents the parent artifacts it extends
   * @param additionalPlugins any plugin classes that need to be explicitly declared because they cannot be found
   *                          by inspecting the jar. This is true for 3rd party plugins, such as jdbc drivers
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @deprecated since 3.4.0
   * Use {@link #addPluginArtifact(ArtifactId, Set, Set, Class, Class[])}
   */
  void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                         @Nullable Set<PluginClass> additionalPlugins,
                         Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Builds an artifact from the specified plugin classes and then adds it. The
   * jar created will include all classes in the same package as the give classes, plus any dependencies of the
   * given classes. If another plugin in the same package as the given plugin requires a different set of dependent
   * classes, you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include specify both functionX and functionY when calling this method.
   *
   * @param artifactId the id of the artifact to add
   * @param parents the parent artifacts it extends
   * @param additionalPlugins any plugin classes that need to be explicitly declared because they cannot be found
   *                          by inspecting the jar. This is true for 3rd party plugins, such as jdbc drivers
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @return {@link ArtifactManager} to manage the added plugin artifact
   */
  ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Deletes the specified artifact.
   *
   * @param artifactId the id of the artifact to delete
   * @deprecated since 3.4.0. Use {@link ArtifactManager#delete()} instead
   */
  @Deprecated
  void deleteArtifact(Id.Artifact artifactId) throws Exception;

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
   * Gets Dataset manager of Dataset instance of type {@literal <}T>.
   *
   * @param namespace namespace of the dataset
   * @param datasetInstanceName instance name of dataset
   * @return Dataset Manager of Dataset instance of type {@literal <}T>
   * @throws Exception
   */
  @Beta
  <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception;

  /**
   * @param namespace namespace to interact within
   * @return a JDBC connection that allows the running of SQL queries over data sets
   */
  @Beta
  Connection getQueryClient(Id.Namespace namespace) throws Exception;

  /**
   * Creates a namespace.
   *
   * @param namespaceMeta the namespace to create
   * @deprecated since 3.4.0. Use {@link NamespaceAdmin} exposed by TestBase for namespace operations.
   */
  @Deprecated
  void createNamespace(NamespaceMeta namespaceMeta) throws Exception;

  /**
   * Deletes a namespace.
   *
   * @param namespace the namespace to delete
   * @deprecated since 3.4.0. Use {@link NamespaceAdmin} exposed by TestBase for namespace operations.
   */
  @Deprecated
  void deleteNamespace(Id.Namespace namespace) throws Exception;

  /**
   * Returns a {@link StreamManager} for the specified {@link Id.Stream}.
   */
  StreamManager getStreamManager(Id.Stream streamId);

  /**
   * Removes all apps in the specified namespace.
   *
   * @param namespaceId the namespace from which to remove all apps
   */
  void deleteAllApplications(NamespaceId namespaceId) throws Exception;
}
