/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.InMemoryProgramRunnerModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.common.namespace.LocalNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.InMemoryStreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamViewHttpHandler;
import co.cask.cdap.data.stream.service.BasicStreamWriterSizeCollector;
import co.cask.cdap.data.stream.service.LocalStreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamFetchHandler;
import co.cask.cdap.data.stream.service.StreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data.stream.service.StreamWriterSizeCollector;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.transaction.stream.FileStreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.DefaultApplicationManager;
import co.cask.cdap.test.internal.DefaultStreamManager;
import co.cask.cdap.test.internal.DefaultStreamWriter;
import co.cask.cdap.test.internal.LocalStreamWriter;
import co.cask.cdap.test.internal.StreamManagerFactory;
import co.cask.cdap.test.internal.StreamWriterFactory;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Base class to inherit from, provides testing functionality for {@link co.cask.cdap.api.app.Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 *
 * @deprecated Unit-test classes should inherit from {@link TestBase}.
 * @see TestBase
 */
@Deprecated
public class ConfigurableTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurableTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static CConfiguration cConf;
  private static int startCount;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static SchedulerService schedulerService;
  private static ExploreExecutorService exploreExecutorService;
  private static ExploreClient exploreClient;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static TransactionManager txService;
  private static StreamCoordinatorClient streamCoordinatorClient;
  private static MetricsManager metricsManager;

  // This list is to record ApplicationManager create inside @Test method
  private static final List<ApplicationManager> applicationManagers = Lists.newArrayList();

  private static TestManager testManager;
  private static NamespaceAdmin namespaceAdmin;

  private static TestManager getTestManager() {
    Preconditions.checkState(testManager != null, "Test framework is not yet running");
    return testManager;
  }

  @Before
  public void beforeTest() throws Exception {
    applicationManagers.clear();
  }

  /**
   * By default after each test finished, it will stop all apps started during the test.
   * Sub-classes can override this method to provide different behavior.
   */
  @After
  public void afterTest() throws Exception {
    for (ApplicationManager manager : applicationManagers) {
      manager.stopAll();
    }
  }

  /**
   * This should be called by the subclasses to initialize the test base.
   *
   * @deprecated Use {@link TestConfiguration} instead.
   */
  @Deprecated
  protected static void initTestBase(@Nullable Map<String, String> additionalConfiguration) throws Exception {
    initialize(additionalConfiguration);
  }

  private static void initialize(@Nullable Map<String, String> additionalConfiguration) throws Exception {
    if (startCount++ > 0) {
      return;
    }
    File localDataDir = tmpFolder.newFolder();

    cConf = createCConf(localDataDir, additionalConfiguration);

    org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {
      File tmpDir = tmpFolder.newFolder();
      File binDir = new File(tmpDir, "bin");
      Assert.assertTrue(binDir.mkdirs());

      copyTempFile("hadoop.dll", tmpDir);
      copyTempFile("winutils.exe", binDir);
      System.setProperty("hadoop.home.dir", tmpDir.getAbsolutePath());
      System.load(new File(tmpDir, "hadoop.dll").getAbsolutePath());
    }

    Injector injector = Guice.createInjector(
      createDataFabricModule(),
      new DataSetsModules().getStandaloneModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ServiceStoreModules().getInMemoryModules(),
      new InMemoryProgramRunnerModule(LocalStreamWriter.class),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamHandler.class).in(Scopes.SINGLETON);
          bind(StreamFetchHandler.class).in(Scopes.SINGLETON);
          bind(StreamViewHttpHandler.class).in(Scopes.SINGLETON);
          bind(AbstractNamespaceClient.class).to(LocalNamespaceClient.class).in(Scopes.SINGLETON);
          bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);
          bind(StreamWriterSizeCollector.class).to(BasicStreamWriterSizeCollector.class).in(Scopes.SINGLETON);
          bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class).in(Scopes.SINGLETON);
          bind(MetricsManager.class).toProvider(MetricsManagerProvider.class);
        }
      },
      // todo: do we need handler?
      new MetricsHandlerModule(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
      new NotificationServiceRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        @SuppressWarnings("deprecation")
        protected void configure() {
          install(new FactoryModuleBuilder().implement(ApplicationManager.class, DefaultApplicationManager.class)
                    .build(ApplicationManagerFactory.class));
          install(new FactoryModuleBuilder().implement(StreamWriter.class, DefaultStreamWriter.class)
                    .build(StreamWriterFactory.class));
          install(new FactoryModuleBuilder().implement(StreamManager.class, DefaultStreamManager.class)
                    .build(StreamManagerFactory.class));
          bind(TemporaryFolder.class).toInstance(tmpFolder);
        }
      }
    );

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    metricsQueryService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    schedulerService = injector.getInstance(SchedulerService.class);
    schedulerService.startAndWait();
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
      exploreExecutorService.startAndWait();
      exploreClient = injector.getInstance(ExploreClient.class);
    }
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();
    testManager = injector.getInstance(UnitTestManager.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    // we use MetricStore directly, until RuntimeStats API changes
    RuntimeStats.metricStore = injector.getInstance(MetricStore.class);
    metricsManager = injector.getInstance(MetricsManager.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
  }

  private static class MetricsManagerProvider implements Provider<MetricsManager> {
    private final MetricStore metricStore;

    @Inject
    private MetricsManagerProvider(MetricStore metricStore) {
      this.metricStore = metricStore;
    }

    @Override
    public MetricsManager get() {
      return new MetricsManager(metricStore);
    }
  }

  private static CConfiguration createCConf(File localDataDir,
                                            Map<String, String> additionalConfiguration) throws IOException {
    CConfiguration cConf = CConfiguration.create();

    // Setup defaults that can be overridden by user
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(Constants.Explore.START_ON_DEMAND, true);

    // This is the deprecated way where configurations are passed to the initTestBase method.
    // Have it here for backward compatibility.
    if (additionalConfiguration != null) {
      for (Map.Entry<String, String> entry : additionalConfiguration.entrySet()) {
        cConf.set(entry.getKey(), entry.getValue());
        LOG.info("Additional configuration set: {} = {}", entry.getKey(), entry.getValue());
      }
    }

    // Setup test case specific configurations.
    // The system properties are usually setup by TestConfiguration class using @ClassRule
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(TestConfiguration.PROPERTY_PREFIX)) {
        String value = System.getProperty(key);
        cConf.set(key.substring(TestConfiguration.PROPERTY_PREFIX.length()), System.getProperty(key));
        LOG.info("Custom configuration set: {} = {}", key, value);
      }
    }

    // These configurations cannot be overridden by user
    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.set(Constants.Metrics.SERVER_PORT, Integer.toString(Networks.getRandomPort()));

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.set(Constants.Explore.LOCAL_DATA_DIR, tmpFolder.newFolder("hive").getAbsolutePath());
    return cConf;
  }

  private static Module createDataFabricModule() {
    return Modules.override(new DataFabricModules().getInMemoryModules(),
                            new ViewAdminModules().getInMemoryModules(),
                            new StreamAdminModules().getInMemoryModules())
      .with(new AbstractModule() {

        @Override
        protected void configure() {
          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(FileStreamAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
        }
      });
  }

  private static void copyTempFile(String infileName, File outDir) throws IOException {
    URL url = TestBase.class.getClassLoader().getResource(infileName);
    if (url == null) {
      throw new IOException("Failed to get resource for " + infileName);
    }
    File outFile = new File(outDir, infileName);
    ByteStreams.copy(Resources.newInputStreamSupplier(url), Files.newOutputStreamSupplier(outFile));
  }

  @AfterClass
  public static void finish() throws Exception {
    if (--startCount != 0) {
      return;
    }

    namespaceAdmin.delete(Id.Namespace.DEFAULT);
    streamCoordinatorClient.stopAndWait();
    metricsQueryService.stopAndWait();
    metricsCollectionService.startAndWait();
    schedulerService.stopAndWait();
    if (exploreClient != null) {
      Closeables.closeQuietly(exploreClient);
    }
    if (exploreExecutorService != null) {
      exploreExecutorService.stopAndWait();
    }
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
  }

  protected MetricsManager getMetricsManager() {
    return metricsManager;
  }

  /**
   * Creates a Namespace.
   *
   * @param namespace the namespace to create
   * @throws Exception
   */
  protected static void createNamespace(Id.Namespace namespace) throws Exception {
    getTestManager().createNamespace(new NamespaceMeta.Builder().setName(namespace).build());
  }

  /**
   * Deletes a Namespace.
   *
   * @param namespace the namespace to create
   * @throws Exception
   */
  protected static void deleteNamespace(Id.Namespace namespace) throws Exception {
    getTestManager().deleteNamespace(namespace);
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Id.Namespace namespace,
                                                        Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  protected static ApplicationManager deployApplication(Id.Namespace namespace,
                                                        Class<? extends Application> applicationClz, Config appConfig,
                                                        File... bundleEmbeddedJars) {
    ApplicationManager applicationManager = getTestManager().deployApplication(namespace, applicationClz, appConfig,
                                                                               bundleEmbeddedJars);
    applicationManagers.add(applicationManager);
    return applicationManager;
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(Id.Namespace.DEFAULT, applicationClz, bundleEmbeddedJars);
  }

  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz, Config appConfig,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(Id.Namespace.DEFAULT, applicationClz, appConfig, bundleEmbeddedJars);
  }

  /**
   * Deploys an {@link Application}. The application artifact must already exist.
   *
   * @param appId the id of the application to create
   * @param appRequest the application create or update request
   * @return An {@link ApplicationManager} to manage the deployed application
   */
  protected static ApplicationManager deployApplication(Id.Application appId,
                                                        AppRequest appRequest) throws Exception {
    return getTestManager().deployApplication(appId, appRequest);
  }

  /**
   * Add the specified artifact.
   *
   * @param artifactId the id of the artifact to add
   * @param artifactFile the contents of the artifact. Must be a valid jar file containing apps or plugins
   * @throws Exception
   */
  protected static void addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    getTestManager().addArtifact(artifactId, artifactFile);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @throws Exception
   */
  protected static void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    getTestManager().addAppArtifact(artifactId, appClass);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param exportPackages the packages to export and place in the manifest of the jar to build. This should include
   *                       packages that contain classes that plugins for the application will implement.
   * @throws Exception
   */
  protected static void addAppArtifact(Id.Artifact artifactId, Class<?> appClass,
                                       String... exportPackages) throws Exception {
    getTestManager().addAppArtifact(artifactId, appClass, exportPackages);
  }

  /**
   * Build an application artifact from the specified class and then add it.
   *
   * @param artifactId the id of the artifact to add
   * @param appClass the application class to build the artifact from
   * @param manifest the manifest to use when building the jar
   * @throws Exception
   */
  protected static void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception {
    getTestManager().addAppArtifact(artifactId, appClass, manifest);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
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
   * @throws Exception
   */
  protected static void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                          Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    getTestManager().addPluginArtifact(artifactId, parent, pluginClass, pluginClasses);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
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
   * @throws Exception
   */
  protected static void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                          Set<PluginClass> additionalPlugins,
                                          Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    getTestManager().addPluginArtifact(artifactId, parent, additionalPlugins, pluginClass, pluginClasses);
  }

  /**
   * Build an artifact from the specified plugin classes and then add it. The
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
   * @param parentArtifacts the parent artifacts it extends
   * @param pluginClass the plugin class to build the jar from
   * @param pluginClasses any additional plugin classes that should be included in the jar
   * @throws Exception
   */
  protected static void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parentArtifacts,
                                          Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    getTestManager().addPluginArtifact(artifactId, parentArtifacts, pluginClass, pluginClasses);
  }

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   */
  protected static void clear() {
    try {
      getTestManager().clear();
    } catch (Exception e) {
      // Unchecked exception to maintain compatibility until we remove this method
      throw Throwables.propagate(e);
    }
  }

  /**
   * Deploys {@link DatasetModule}.
   *
   * @param moduleName name of the module
   * @param datasetModule module class
   * @throws Exception
   */
  protected static void deployDatasetModule(Id.Namespace namespace, String moduleName,
                                            Class<? extends DatasetModule> datasetModule) throws Exception {
    getTestManager().deployDatasetModule(namespace, moduleName, datasetModule);
  }


  /**
   * Deploys {@link DatasetModule}.
   *
   * @param moduleName name of the module
   * @param datasetModule module class
   * @throws Exception
   */
  protected static void deployDatasetModule(String moduleName,
                                            Class<? extends DatasetModule> datasetModule) throws Exception {
    deployDatasetModule(Id.Namespace.DEFAULT, moduleName, datasetModule);
  }

  /**
   * Adds an instance of a dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   */
  protected static <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace, String datasetTypeName,
                                                                 String datasetInstanceName,
                                                                 DatasetProperties props) throws Exception {
    return getTestManager().addDatasetInstance(namespace, datasetTypeName, datasetInstanceName, props);
  }


  /**
   * Adds an instance of a dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   */
  protected static <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                                 String datasetInstanceName,
                                                                 DatasetProperties props) throws Exception {
    return addDatasetInstance(Id.Namespace.DEFAULT, datasetTypeName, datasetInstanceName, props);
  }

  /**
   * Adds an instance of dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param <T> type of the dataset admin
   */
  protected final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace, String datasetTypeName,
                                                                String datasetInstanceName) throws Exception {
    return addDatasetInstance(namespace, datasetTypeName, datasetInstanceName, DatasetProperties.EMPTY);
  }

  /**
   * Adds an instance of dataset.
   *
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param <T> type of the dataset admin
   */
  protected final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                                String datasetInstanceName) throws Exception {
    return addDatasetInstance(Id.Namespace.DEFAULT, datasetTypeName, datasetInstanceName,
                              DatasetProperties.EMPTY);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   *
   * @param datasetInstanceName - instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  protected final <T> DataSetManager<T> getDataset(Id.Namespace namespace,
                                                   String datasetInstanceName) throws Exception {
    return getTestManager().getDataset(namespace, datasetInstanceName);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   *
   * @param datasetInstanceName - instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  protected final <T> DataSetManager<T> getDataset(String datasetInstanceName) throws Exception {
    return getDataset(Id.Namespace.DEFAULT, datasetInstanceName);
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  protected final Connection getQueryClient(Id.Namespace namespace) throws Exception {
    if (!cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      throw new UnsupportedOperationException("Explore service is disabled. QueryClient not supported.");
    }
    return getTestManager().getQueryClient(namespace);
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  protected final Connection getQueryClient() throws Exception {
    return getQueryClient(Id.Namespace.DEFAULT);
  }

  /**
   * Returns a {@link StreamManager} for the specified stream in the default namespace
   *
   * @param streamName the specified stream
   * @return {@link StreamManager} for the specified stream in the default namespace
   */
  protected final StreamManager getStreamManager(String streamName) {
    return getStreamManager(Id.Namespace.DEFAULT, streamName);
  }

  /**
   * Returns a {@link StreamManager} for the specified stream in the specified namespace
   *
   * @param streamName the specified stream
   * @return {@link StreamManager} for the specified stream in the specified namespace
   */
  protected final StreamManager getStreamManager(Id.Namespace namespace, String streamName) {
    return getTestManager().getStreamManager(Id.Stream.from(namespace, streamName));
  }

  protected TransactionManager getTxService() {
    return txService;
  }
}

