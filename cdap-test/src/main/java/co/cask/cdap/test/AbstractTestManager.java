/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;

import java.io.File;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Class that delegates deprecated methods to their non-deprecated counterparts, and delegates methods with
 * fewer arguments to the one with more arguments.
 */
public abstract class AbstractTestManager implements TestManager {

  /* start of deprecated methods */

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace.toEntityId(), applicationClz, bundleEmbeddedJars);
  }

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    return deployApplication(namespace.toEntityId(), applicationClz, configObject, bundleEmbeddedJars);
  }

  @Override
  public ApplicationManager deployApplication(Id.Application appId, AppRequest appRequest) throws Exception {
    return deployApplication(appId.toEntityId(), appRequest);
  }

  @Override
  public ArtifactManager addArtifact(ArtifactId artifactId, File artifactFile) throws Exception {
    return addArtifact(artifactId, artifactFile);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, String... exportPackages) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, exportPackages);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, manifest);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent, Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), pluginClass, pluginClasses);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents, Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parents, pluginClass, pluginClasses);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parents, additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public void deployDatasetModule(Id.Namespace namespace, String moduleName,
                                  Class<? extends DatasetModule> datasetModule) throws Exception {
    deployDatasetModule(namespace.toEntityId().datasetModule(moduleName), datasetModule);
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName,
                                                       String datasetInstanceName,
                                                       DatasetProperties props) throws Exception {
    return addDatasetInstance(datasetTypeName, namespace.toEntityId().dataset(datasetInstanceName), props);
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName,
                                                       String datasetInstanceName) throws Exception {
    return addDatasetInstance(datasetTypeName, namespace.toEntityId().dataset(datasetInstanceName));
  }

  @Override
  public void deleteDatasetInstance(NamespaceId namespaceId, String datasetInstanceName) throws Exception {
    deleteDatasetInstance(namespaceId.dataset(datasetInstanceName));
  }

  @Override
  public <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception {
    return getDataset(namespace.toEntityId().dataset(datasetInstanceName));
  }

  @Override
  public Connection getQueryClient(Id.Namespace namespace) throws Exception {
    return getQueryClient(namespace.toEntityId());
  }

  @Override
  public StreamManager getStreamManager(Id.Stream streamId) {
    return getStreamManager(streamId.toEntityId());
  }

  /* end of deprecated methods, start of methods that just call another method */

  @Override
  public ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, toRange(parent), pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, parents, null, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    return addPluginArtifact(artifactId, toRange(parent), additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId) throws Exception {
    return addDatasetInstance(datasetType, datasetId, DatasetProperties.EMPTY);
  }

  private Set<ArtifactRange> toRange(ArtifactId parent) {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(parent.getParent(), parent.getArtifact(),
                                  new ArtifactVersion(parent.getVersion()), true,
                                  new ArtifactVersion(parent.getVersion()), true));
    return parents;
  }
}
