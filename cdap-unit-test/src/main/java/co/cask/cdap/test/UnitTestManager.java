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
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.MockAppConfigurer;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.StickyEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.ArtifactManagerFactory;
import co.cask.cdap.test.internal.StreamManagerFactory;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * {@link TestManager} for use in unit tests.
 */
public class UnitTestManager implements TestManager {

  private static final Gson GSON = new Gson();
  private final AppFabricClient appFabricClient;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryClient;
  private final ApplicationManagerFactory appManagerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final StreamManagerFactory streamManagerFactory;
  private final LocationFactory locationFactory;
  private final ArtifactRepository artifactRepository;
  private final ArtifactManagerFactory artifactManagerFactory;
  private final MetricsManager metricsManager;
  private final File tmpDir;

  @Inject
  public UnitTestManager(AppFabricClient appFabricClient,
                         DatasetFramework datasetFramework,
                         TransactionSystemClient txSystemClient,
                         DiscoveryServiceClient discoveryClient,
                         ApplicationManagerFactory appManagerFactory,
                         NamespaceAdmin namespaceAdmin,
                         StreamManagerFactory streamManagerFactory,
                         LocationFactory locationFactory,
                         MetricsManager metricsManager,
                         ArtifactRepository artifactRepository,
                         ArtifactManagerFactory artifactManagerFactory,
                         CConfiguration cConf) {
    this.appFabricClient = appFabricClient;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryClient = discoveryClient;
    this.appManagerFactory = appManagerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.streamManagerFactory = streamManagerFactory;
    this.locationFactory = locationFactory;
    this.artifactRepository = artifactRepository;
    // this should have been set to a temp dir during injector setup
    this.metricsManager = metricsManager;
    this.artifactManagerFactory = artifactManagerFactory;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace,
                                              Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");
    String appConfig = "";
    TypeToken typeToken = TypeToken.of(applicationClz);
    TypeToken<?> configToken = typeToken.resolveType(Application.class.getTypeParameters()[0]);

    try {
      if (configObject != null) {
        appConfig = GSON.toJson(configObject);
      } else {
        configObject = ((Class<Config>) configToken.getRawType()).newInstance();
      }

      Application app = applicationClz.newInstance();
      MockAppConfigurer configurer = new MockAppConfigurer(app);
      app.configure(configurer, new DefaultApplicationContext<>(configObject));
      appFabricClient.deployApplication(namespace, applicationClz, appConfig, bundleEmbeddedJars);
      return appManagerFactory.create(Id.Application.from(namespace, configurer.getName()));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationManager deployApplication(Id.Application appId,
                                              AppRequest appRequest) throws Exception {
    appFabricClient.deployApplication(appId, appRequest);
    return appManagerFactory.create(appId);
  }

  @Override
  public ApplicationManager getApplicationManager(Id.Application appId) {
    return appManagerFactory.create(appId);
  }

  @Override
  public void addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    artifactRepository.addArtifact(artifactId, artifactFile);
  }

  @Override
  public ArtifactManager addArtifact(NamespacedArtifactId artifactId, File artifactFile) throws Exception {
    artifactRepository.addArtifact(artifactId.toId(), artifactFile);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass);
  }

  @Override
  public ArtifactManager addAppArtifact(NamespacedArtifactId artifactId, Class<?> appClass) throws Exception {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, new Manifest());
    addArtifact(artifactId, appJar);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, String... exportPackages) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, exportPackages);
  }

  @Override
  public ArtifactManager addAppArtifact(NamespacedArtifactId artifactId, Class<?> appClass,
                                        String... exportPackages) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest);
    addArtifact(artifactId, appJar);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, manifest);
  }

  @Override
  public ArtifactManager addAppArtifact(NamespacedArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest) throws Exception {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest);
    addArtifact(artifactId, appJar);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(NamespacedArtifactId artifactId, NamespacedArtifactId parent,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      Ids.namespace(parent.getNamespace()).toId(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, pluginClass, pluginClasses);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parents, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(NamespacedArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    File pluginJar = createPluginJar(artifactId, pluginClass, pluginClasses);
    artifactRepository.addArtifact(artifactId.toId(), pluginJar, parents);
    Preconditions.checkState(pluginJar.delete());
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(NamespacedArtifactId artifactId, NamespacedArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      Ids.namespace(parent.getNamespace()).toId(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, additionalPlugins, pluginClass, pluginClasses);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {

  }

  @Override
  public ArtifactManager addPluginArtifact(NamespacedArtifactId artifactId, Set<ArtifactRange> parents,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    File pluginJar = createPluginJar(artifactId, pluginClass, pluginClasses);
    artifactRepository.addArtifact(artifactId.toId(), pluginJar, parents,
                                   additionalPlugins, Collections.<String, String>emptyMap());
    Preconditions.checkState(pluginJar.delete());
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    artifactRepository.deleteArtifact(artifactId);
  }

  @Override
  public void clear() throws Exception {
    try {
      appFabricClient.reset();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      metricsManager.resetAll();
    }
  }

  @Beta
  @Override
  public final void deployDatasetModule(Id.Namespace namespace,
                                        String moduleName, Class<? extends DatasetModule> datasetModule)
    throws Exception {
    datasetFramework.addModule(Id.DatasetModule.from(namespace, moduleName), datasetModule.newInstance());
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                             String datasetTypeName, String datasetInstanceName,
                                                             DatasetProperties props) throws Exception {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetInstanceName);
    datasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
    return datasetFramework.getAdmin(datasetInstanceId, null);
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                             String datasetTypeName,
                                                             String datasetInstanceName) throws Exception {
    return addDatasetInstance(namespace, datasetTypeName, datasetInstanceName, DatasetProperties.EMPTY);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   * @param datasetInstanceName - instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  @Beta
  @Override
  public final <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetInstanceName);
    @SuppressWarnings("unchecked")
    final T dataSet = datasetFramework.getDataset(datasetInstanceId, new HashMap<String, String>(), null);
    try {
      final TransactionContext txContext;
      // not every dataset is TransactionAware. FileSets for example, are not transactional.
      if (dataSet instanceof TransactionAware) {
        TransactionAware txAwareDataset = (TransactionAware) dataSet;
        txContext = new TransactionContext(txSystemClient, Lists.newArrayList(txAwareDataset));
        txContext.start();
      } else {
        txContext = null;
      }
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            if (txContext != null) {
              txContext.finish();
              txContext.start();
            }
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  @Beta
  @Override
  public final Connection getQueryClient(Id.Namespace namespace) throws Exception {
    // this makes sure the Explore JDBC driver is loaded
    Class.forName(ExploreDriver.class.getName());

    Discoverable discoverable = new StickyEndpointStrategy(
      discoveryClient.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();

    if (null == discoverable) {
      throw new IOException("Explore service could not be discovered.");
    }

    InetSocketAddress address = discoverable.getSocketAddress();
    String host = address.getHostName();
    int port = address.getPort();

    String connectString = String.format("%s%s:%d?namespace=%s", Constants.Explore.Jdbc.URL_PREFIX, host, port,
      namespace.getId());

    return DriverManager.getConnection(connectString);
  }

  @Override
  public void createNamespace(NamespaceMeta namespaceMeta) throws Exception {
    namespaceAdmin.create(namespaceMeta);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespace) throws Exception {
    namespaceAdmin.delete(namespace);
  }

  @Override
  public StreamManager getStreamManager(Id.Stream streamId) {
    return streamManagerFactory.create(streamId);
  }

  @Override
  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    appFabricClient.deleteAllApplications(namespaceId);
  }

  private Manifest createManifest(Class<?> cls, Class<?>... classes) {
    Manifest manifest = new Manifest();
    Set<String> exportPackages = new HashSet<>();
    exportPackages.add(cls.getPackage().getName());
    for (Class<?> clz : classes) {
      exportPackages.add(clz.getPackage().getName());
    }

    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    return manifest;
  }

  private File createPluginJar(NamespacedArtifactId artifactId, Class<?> pluginClass,
                               Class<?>... pluginClasses) throws IOException {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), destination);
    appJar.delete();
    return destination;
  }

  private void addArtifact(NamespacedArtifactId artifactId, Location jar) throws Exception {
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Files.copy(Locations.newInputSupplier(jar), destination);
    jar.delete();

    artifactRepository.addArtifact(artifactId.toId(), destination);
    Preconditions.checkState(destination.delete());
  }
}
