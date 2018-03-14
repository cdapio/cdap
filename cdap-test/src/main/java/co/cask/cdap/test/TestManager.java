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
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;

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
  ApplicationManager deployApplication(NamespaceId namespace,
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
  ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                       @Nullable Config configObject, File... bundleEmbeddedJars);

  /**
   * Creates an {@link Application} with a version using an existing artifact.
   *
   * @param appId the id of the application to create
   * @param appRequest the app create or update request that includes the artifact to create the app from and any config
   *                   to pass to the application.
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  ApplicationManager deployApplication(ApplicationId appId, AppRequest appRequest) throws Exception;

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
   * @return an {@link ArtifactManager} to manage the added artifact
   */
  ArtifactManager addArtifact(ArtifactId artifactId, File artifactFile) throws Exception;

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
   * @return {@link ArtifactManager} to manage the added plugin artifact
   */
  ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                    @Nullable Set<PluginClass> additionalPlugins,
                                    Class<?> pluginClass, Class<?>... pluginClasses) throws Exception;

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   */
  void clear() throws Exception;

  /**
   * Deploys {@link DatasetModule}.
   *
   * @param datasetModuleId the dataset module id
   * @param datasetModule module class
   * @throws Exception
   */
  @Beta
  void deployDatasetModule(DatasetModuleId datasetModuleId, Class<? extends DatasetModule> datasetModule)
    throws Exception;

  /**
   * Adds an instance of a dataset.
   *
   * @param datasetType the dataset type, like "table" or "fileset"
   * @param datasetId the dataset id
   * @param props the dataset properties
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId,
                                                DatasetProperties props) throws Exception;

  /**
   * Adds an instance of dataset.
   *
   * @param datasetType the dataset type, like "table" or "fileset"
   * @param datasetId the dataset id
   * @param <T> type of the dataset admin
   * @return a DatasetAdmin to manage the dataset instance
   */
  <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId) throws Exception;

  /**
   * Deletes an instance of dataset
   *
   * @param datasetId the dataset to delete
   * @throws Exception
   */
  void deleteDatasetInstance(DatasetId datasetId) throws Exception;

  /**
   * Gets Dataset manager of Dataset instance of type {@literal <}T>.
   *
   * @param datasetId the id of the dataset
   * @return Dataset Manager of Dataset instance of type {@literal <}T>
   * @throws Exception
   */
  <T> DataSetManager<T> getDataset(DatasetId datasetId) throws Exception;

  /**
   * @param namespace namespace to interact within
   * @return a JDBC connection that allows the running of SQL queries over data sets
   */
  Connection getQueryClient(NamespaceId namespace) throws Exception;

  /**
   * Returns a {@link StreamManager} for the specified {@link Id.Stream}.
   */
  StreamManager getStreamManager(StreamId streamId);

  /**
   * Removes all apps in the specified namespace.
   *
   * @param namespaceId the namespace from which to remove all apps
   */
  void deleteAllApplications(NamespaceId namespaceId) throws Exception;

  /**
   * Get the app detail of an application
   *
   * @param applicationId the app id of the application
   */
  ApplicationDetail getApplicationDetail(ApplicationId applicationId) throws Exception;

  /**
   * Add a new schedule to an existing application
   *
   * @param scheduleId the ID of the schedule to add
   * @param scheduleDetail the {@link ScheduleDetail} describing the new schedule.
   */
  void addSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception;

  /**
   * Update an existing schedule.
   *
   * @param scheduleId the ID of the schedule to add
   * @param scheduleDetail the {@link ScheduleDetail} describing the updated schedule.
   */
  void updateSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception;

  /**
   * Delete an existing schedule.
   *
   * @param scheduleId the ID of the schedule to be deleted
   */
  void deleteSchedule(ScheduleId scheduleId) throws Exception;
}
