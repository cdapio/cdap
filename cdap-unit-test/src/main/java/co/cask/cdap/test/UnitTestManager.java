/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.MockAppConfigurer;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.StickyEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramResources;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.test.PluginJarHelper;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.ArtifactManagerFactory;
import co.cask.cdap.test.internal.StreamManagerFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * {@link TestManager} for use in unit tests.
 */
public class UnitTestManager extends AbstractTestManager {

  private static final Logger LOG = LoggerFactory.getLogger(TestBase.class);

  private static final ClassAcceptor CLASS_ACCEPTOR = new ClassAcceptor() {
    final Set<String> visibleResources = ProgramResources.getVisibleResources();
    private final AtomicBoolean logWarnOnce = new AtomicBoolean();

    @Override
    public boolean accept(String className, URL classUrl, URL classPathUrl) {
      String resourceName = className.replace('.', '/') + ".class";
      if (visibleResources.contains(resourceName)) {
        return false;
      }
      // Always includes Scala class. It is for CDAP-5168
      if (resourceName.startsWith("scala/")) {
        return true;
      }
      try {
        // allow developers to exclude spark-core module from their unit tests (it'll work if they don't use spark)
        getClass().getClassLoader().loadClass("co.cask.cdap.app.runtime.spark.SparkRuntimeUtils");
        // If it is loading by spark framework, don't include it in the app JAR
        return !SparkRuntimeUtils.SPARK_PROGRAM_CLASS_LOADER_FILTER.acceptResource(resourceName);
      } catch (ClassNotFoundException e) {
        if (logWarnOnce.compareAndSet(false, true)) {
          LOG.warn("Spark will not be available for unit tests.");
        }
        return true;
      }
    }
  };

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

  @Override
  public ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");
    Type configType = Artifacts.getConfigType(applicationClz);

    try {
      ArtifactId artifactId = new ArtifactId(namespace.getNamespace(), applicationClz.getSimpleName(), "1.0-SNAPSHOT");
      addAppArtifact(artifactId, applicationClz);

      if (configObject == null) {
        configObject = (Config) TypeToken.of(configType).getRawType().newInstance();
      }

      Application app = applicationClz.newInstance();
      MockAppConfigurer configurer = new MockAppConfigurer(app);
      app.configure(configurer, new DefaultApplicationContext<>(configObject));
      ApplicationId applicationId = new ApplicationId(namespace.getNamespace(), configurer.getName());

      ArtifactSummary artifactSummary = new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion());
      appFabricClient.deployApplication(applicationId.toId(),
                                        new AppRequest(artifactSummary, configObject));
      return appManagerFactory.create(applicationId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationManager deployApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    appFabricClient.deployApplication(appId, appRequest);
    return appManagerFactory.create(appId);
  }

  @Override
  public ApplicationManager getApplicationManager(ApplicationId applicationId) {
    return appManagerFactory.create(applicationId);
  }
  @Override
  public ArtifactManager addArtifact(ArtifactId artifactId, File artifactFile) throws Exception {
    artifactRepository.addArtifact(artifactId.toId(), artifactFile);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass) throws Exception {
    return addAppArtifact(artifactId, appClass, new String[0]);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        String... exportPackages) throws Exception {
    Manifest manifest = new Manifest();
    if (exportPackages.length > 0) {
      manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    }
    return addAppArtifact(artifactId, appClass, manifest);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest) throws Exception {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest, CLASS_ACCEPTOR);
    addArtifact(artifactId, appJar);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getParent().getNamespace(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, pluginClass, pluginClasses);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    File pluginJar = createPluginJar(artifactId, pluginClass, pluginClasses);
    artifactRepository.addArtifact(artifactId.toId(), pluginJar, parents, null);
    Preconditions.checkState(pluginJar.delete());
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getParent().getNamespace(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, additionalPlugins, pluginClass, pluginClasses);
    return artifactManagerFactory.create(artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
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

  @Override
  public void deployDatasetModule(DatasetModuleId datasetModuleId,
                                  Class<? extends DatasetModule> datasetModule) throws Exception {
    datasetFramework.addModule(datasetModuleId, datasetModule.newInstance());
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId,
                                                       DatasetProperties props) throws Exception {
    datasetFramework.addInstance(datasetType, datasetId, props);
    return datasetFramework.getAdmin(datasetId, null);
  }

  @Override
  public void deleteDatasetInstance(DatasetId datasetId) throws Exception {
    datasetFramework.deleteInstance(datasetId);
  }

  @Override
  public <T> DataSetManager<T> getDataset(DatasetId datasetInstanceId) throws Exception {
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
      return new UnitTestDatasetManager<>(dataSet, txContext);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Dataset manager used in unit tests. Allows to execute a runnable in a transaction.
   *
   * @param <T> type of the dataset
   */
  // TODO (CDAP-3792): remove this hack when TestBase has better support for transactions
  public class UnitTestDatasetManager<T> implements DataSetManager<T> {
    private final T dataset;
    private final TransactionContext txContext;

    UnitTestDatasetManager(T dataset, TransactionContext txContext) {
      this.dataset = dataset;
      this.txContext = txContext;
    }

    @Override
    public T get() {
      return dataset;
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

    public void execute(Runnable runnable) throws TransactionFailureException {
      TransactionContext txCtx = new TransactionContext(txSystemClient, (TransactionAware) dataset);
      txCtx.start();
      try {
        runnable.run();
      } catch (Throwable t) {
        txCtx.abort(new TransactionFailureException("runnable failed", t));
      }
      txCtx.finish();
    }
  }

  @Override
  public Connection getQueryClient(NamespaceId namespace) throws Exception {
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
                                         namespace.getNamespace());

    return DriverManager.getConnection(connectString);
  }

  @Override
  public void createNamespace(NamespaceMeta namespaceMeta) throws Exception {
    namespaceAdmin.create(namespaceMeta);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespace) throws Exception {
    namespaceAdmin.delete(namespace.toEntityId());
  }

  @Override
  public StreamManager getStreamManager(StreamId streamId) {
    return streamManagerFactory.create(streamId.toId());
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

  private File createPluginJar(ArtifactId artifactId, Class<?> pluginClass,
                               Class<?>... pluginClasses) throws IOException {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), destination);
    appJar.delete();
    return destination;
  }

  private void addArtifact(ArtifactId artifactId, Location jar) throws Exception {
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Files.copy(Locations.newInputSupplier(jar), destination);
    jar.delete();

    artifactRepository.addArtifact(artifactId.toId(), destination);
    Preconditions.checkState(destination.delete());
  }
}
