/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.DefaultApplicationContext;
import io.cdap.cdap.app.MockAppConfigurer;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.runtime.spark.SparkResourceFilters;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.StickyEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.ProgramResources;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.explore.jdbc.ExploreConnectionParams;
import io.cdap.cdap.explore.jdbc.ExploreDriver;
import io.cdap.cdap.internal.AppFabricClient;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.security.spi.AccessException;
import io.cdap.cdap.test.internal.ApplicationManagerFactory;
import io.cdap.cdap.test.internal.ArtifactManagerFactory;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        getClass().getClassLoader().loadClass("io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils");
        // If it is loading by spark framework, don't include it in the app JAR
        return !SparkResourceFilters.SPARK_PROGRAM_CLASS_LOADER_FILTER.acceptResource(resourceName);
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
  private final LocationFactory locationFactory;
  private final ArtifactRepository artifactRepository;
  private final ArtifactManagerFactory artifactManagerFactory;
  private final MetricsManager metricsManager;
  private final File tmpDir;
  private final Cache<ArtifactKey, Location> deploymentJarCache;

  @Inject
  public UnitTestManager(AppFabricClient appFabricClient,
                         DatasetFramework datasetFramework,
                         TransactionSystemClient txSystemClient,
                         DiscoveryServiceClient discoveryClient,
                         ApplicationManagerFactory appManagerFactory,
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
    this.locationFactory = locationFactory;
    this.artifactRepository = artifactRepository;
    // this should have been set to a temp dir during injector setup
    this.metricsManager = metricsManager;
    this.artifactManagerFactory = artifactManagerFactory;
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.deploymentJarCache = CacheBuilder.newBuilder().build();
  }

  @Override
  public ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars)
    throws AccessException {
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");
    Type configType = Artifacts.getConfigType(applicationClz);

    try {
      ArtifactId artifactId = new ArtifactId(namespace.getNamespace(), applicationClz.getSimpleName(), "1.0-SNAPSHOT");
      addAppArtifact(artifactId, applicationClz, new Manifest(), bundleEmbeddedJars);

      if (configObject == null) {
        configObject = (Config) TypeToken.of(configType).getRawType().newInstance();
      }

      Application app = applicationClz.newInstance();
      MockAppConfigurer configurer = new MockAppConfigurer(app);
      app.configure(configurer, new DefaultApplicationContext<>(configObject));
      ApplicationId applicationId = new ApplicationId(namespace.getNamespace(), configurer.getName());

      ArtifactSummary artifactSummary = new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion());
      appFabricClient.deployApplication(Id.Application.fromEntityId(applicationId),
                                        new AppRequest(artifactSummary, configObject));
      return appManagerFactory.create(applicationId);
    } catch (AccessException e) {
      throw e;
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
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(artifactId), artifactFile);
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
    return addAppArtifact(artifactId, appClass, manifest, new File[0]);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest, File... bundleEmbeddedJars) throws Exception {

    Location appJar = deploymentJarCache.get(new AppArtifactKey(appClass, manifest, bundleEmbeddedJars), () ->
      AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest, CLASS_ACCEPTOR, bundleEmbeddedJars));
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
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(artifactId), pluginJar, parents, null);
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
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(artifactId), pluginJar, parents,
                                   additionalPlugins, Collections.<String, String>emptyMap());
    Preconditions.checkState(pluginJar.delete());
    return artifactManagerFactory.create(artifactId);
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

    Discoverable discoverable = new StickyEndpointStrategy(() ->
      discoveryClient.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();

    if (null == discoverable) {
      throw new IOException("Explore service could not be discovered.");
    }

    InetSocketAddress address = discoverable.getSocketAddress();
    String host = address.getHostName();
    int port = address.getPort();

    Map<String, String> params = new HashMap<>();
    params.put(ExploreConnectionParams.Info.NAMESPACE.getName(), namespace.getNamespace());
    params.put(ExploreConnectionParams.Info.SSL_ENABLED.getName(),
               Boolean.toString(URIScheme.HTTPS.isMatch(discoverable)));
    params.put(ExploreConnectionParams.Info.VERIFY_SSL_CERT.getName(), Boolean.toString(false));

    String connectString = String.format("%s%s:%d?%s", Constants.Explore.Jdbc.URL_PREFIX, host, port,
                                         Joiner.on('&').withKeyValueSeparator("=").join(params));

    return DriverManager.getConnection(connectString);
  }

  @Override
  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    appFabricClient.deleteAllApplications(namespaceId);
  }

  @Override
  public ApplicationDetail getApplicationDetail(ApplicationId applicationId) throws Exception {
    return appFabricClient.getVersionedInfo(applicationId);
  }

  @Override
  public void addSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    appFabricClient.addSchedule(scheduleId.getParent(), scheduleDetail);
  }

  @Override
  public void updateSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    appFabricClient.updateSchedule(scheduleId, scheduleDetail);
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId) throws Exception {
    appFabricClient.deleteSchedule(scheduleId);
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
                               Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    Location appJar = deploymentJarCache.get(new PluginArtifactKey(pluginClass, pluginClasses), () ->
      PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses));
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Locations.linkOrCopyOverwrite(appJar, destination);
    return destination;
  }

  private void addArtifact(ArtifactId artifactId, Location jar) throws Exception {
    File destination =
      new File(tmpDir, String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Locations.linkOrCopyOverwrite(jar, destination);

    artifactRepository.addArtifact(Id.Artifact.fromEntityId(artifactId), destination);
    Preconditions.checkState(destination.delete());
  }

  /**
   * Marker interface for {@link #deploymentJarCache} keys
   */
  private interface ArtifactKey {
  }

  /**
   * {@link #deploymentJarCache} key for artifacts produced by {@link #addAppArtifact}
   */
  private static class AppArtifactKey implements ArtifactKey {
    private final Class<?> appClass;
    private final Manifest manifest;
    private final List<File> bundleEmbeddedJars;

    private AppArtifactKey(Class<?> appClass, Manifest manifest, File... bundleEmbeddedJars) {
      this.appClass = appClass;
      this.manifest = manifest;
      this.bundleEmbeddedJars = Arrays.asList(bundleEmbeddedJars);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AppArtifactKey that = (AppArtifactKey) o;
      return Objects.equals(appClass, that.appClass) &&
        Objects.equals(manifest, that.manifest) &&
        Objects.equals(bundleEmbeddedJars, that.bundleEmbeddedJars);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appClass, manifest, bundleEmbeddedJars);
    }
  }

  /**
   * {@link #deploymentJarCache} key for artifacts produced by {@link #createPluginJar}
   */
  private static class PluginArtifactKey implements ArtifactKey {
    private final Class<?> pluginClass;
    private final List<Class<?>> pluginClasses;

    private PluginArtifactKey(Class<?> pluginClass, Class<?>... pluginClasses) {
      this.pluginClass = pluginClass;
      this.pluginClasses = Arrays.asList(pluginClasses);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PluginArtifactKey that = (PluginArtifactKey) o;
      return Objects.equals(pluginClass, that.pluginClass) &&
        Objects.equals(pluginClasses, that.pluginClasses);
    }

    @Override
    public int hashCode() {
      return Objects.hash(pluginClass, pluginClasses);
    }
  }

}
