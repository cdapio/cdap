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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.InMemoryProgramRunnerModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
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
import co.cask.cdap.data.stream.service.BasicStreamWriterSizeCollector;
import co.cask.cdap.data.stream.service.LocalStreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamFetchHandler;
import co.cask.cdap.data.stream.service.StreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data.stream.service.StreamWriterSizeCollector;
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
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.DefaultApplicationManager;
import co.cask.cdap.test.internal.DefaultStreamManager;
import co.cask.cdap.test.internal.DefaultStreamWriter;
import co.cask.cdap.test.internal.LocalNamespaceClient;
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
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
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

/**
 * Base class to inherit from, provides testing functionality for {@link co.cask.cdap.api.app.Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 */
public class ConfigurableTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurableTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

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
   */
  protected static void initTestBase(Map<String, String> additionalConfiguration) throws Exception {
    if (startCount++ > 0) {
      return;
    }
    File localDataDir = tmpFolder.newFolder();
    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.set(Constants.Metrics.SERVER_PORT, Integer.toString(Networks.getRandomPort()));

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(Constants.Explore.START_ON_DEMAND, true);
    cConf.setBoolean(Constants.Scheduler.SCHEDULERS_LAZY_START, true);
    cConf.set(Constants.Explore.LOCAL_DATA_DIR,
              tmpFolder.newFolder("hive").getAbsolutePath());
    cConf.set(Constants.AppFabric.APP_TEMPLATE_DIR, tmpFolder.newFolder("templates").getAbsolutePath());

    if (additionalConfiguration != null) {
      for (Map.Entry<String, String> entry : additionalConfiguration.entrySet()) {
        cConf.set(entry.getKey(), entry.getValue());
        LOG.info("Additional configuration set: " + entry.getKey() + " = " + entry.getValue());
      }
    }

    Configuration hConf = new Configuration();
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
      new AuthModule(),
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
          bind(AbstractNamespaceClient.class).to(LocalNamespaceClient.class).in(Scopes.SINGLETON);
          bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);
          bind(StreamWriterSizeCollector.class).to(BasicStreamWriterSizeCollector.class).in(Scopes.SINGLETON);
          bind(StreamCoordinatorClient.class).to(InMemoryStreamCoordinatorClient.class).in(Scopes.SINGLETON);
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
    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();
    exploreClient = injector.getInstance(ExploreClient.class);
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();
    testManager = injector.getInstance(UnitTestManager.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    // we use MetricStore directly, until RuntimeStats API changes
    RuntimeStats.metricStore = injector.getInstance(MetricStore.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
  }

  private static Module createDataFabricModule() {
    return Modules.override(new DataFabricModules().getInMemoryModules(), new StreamAdminModules().getInMemoryModules())
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
  public static void finish() throws NotFoundException, NamespaceCannotBeDeletedException {
    if (--startCount != 0) {
      return;
    }

    namespaceAdmin.deleteNamespace(Constants.DEFAULT_NAMESPACE_ID);
    streamCoordinatorClient.stopAndWait();
    metricsQueryService.stopAndWait();
    metricsCollectionService.startAndWait();
    schedulerService.stopAndWait();
    Closeables.closeQuietly(exploreClient);
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
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
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Id.Namespace namespace,
                                                        Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    ApplicationManager applicationManager = getTestManager().deployApplication(namespace, applicationClz,
                                                                               bundleEmbeddedJars);
    applicationManagers.add(applicationManager);
    return applicationManager;
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(Constants.DEFAULT_NAMESPACE_ID, applicationClz, bundleEmbeddedJars);
  }

  /**
   * Creates an adapter.
   *
   * @param adapterId The id of the adapter to create
   * @param adapterConfig The configuration for the adapter
   * @return An {@link AdapterManager} to manage the deployed adapter.
   * @throws Exception if there was an exception deploying the adapter.
   */
  protected static AdapterManager createAdapter(Id.Adapter adapterId, AdapterConfig adapterConfig) throws Exception {
    return getTestManager().createAdapter(adapterId, adapterConfig);
  }

  /**
   * Deploys an {@link ApplicationTemplate}.
   *
   * @param namespace The namespace to deploy to
   * @param templateId The id of the template. Must match the name set in
   *                   {@link ApplicationTemplate#configure(ApplicationConfigurer, ApplicationContext)}
   * @param templateClz The template class
   * @param exportPackages The list of packages that should be visible to template plugins. For example,
   *                       if your plugins implement an interface com.company.api.myinterface that is in your template,
   *                       you will want to include 'com.company.api' in the list of export pacakges.
   */
  protected static void deployTemplate(Id.Namespace namespace, Id.ApplicationTemplate templateId,
                                       Class<? extends ApplicationTemplate> templateClz,
                                       String... exportPackages) throws IOException {
    getTestManager().deployTemplate(namespace, templateId, templateClz, exportPackages);
  }

  /**
   * Adds a plugins jar usable by the given template. Only supported in unit tests. Plugins added will not be visible
   * until a call to {@link #deployTemplate(Id.Namespace, Id.ApplicationTemplate, Class, String...)} is made. The
   * jar created will include all classes in the same package as the given plugin class, plus any dependencies of the
   * that class and given additional classes.
   * If another plugin in the same package as the given plugin requires a different set of dependent classes,
   * you must include both plugins. For example, suppose you have two plugins,
   * com.company.myapp.functions.functionX and com.company.myapp.function.functionY, with functionX having
   * one set of dependencies and functionY having another set of dependencies. If you only add functionX, functionY
   * will also be included in the created jar since it is in the same package. However, only functionX's dependencies
   * will be traced and added to the jar, so you will run into issues when the platform tries to register functionY.
   * In this scenario, you must be certain to include functionY in the list of classes when specifying functionX as the
   * plugin class.
   *
   * @param templateId The id of the template to add the plugin for
   * @param jarName The name to use for the plugin jar
   * @param pluginClz A plugin class to add to the jar. Any other class that shares the same package will be included
   *                  in the jar.
   * @param classes Additional plugin classes whose dependencies should be added to the jar.
   * @throws IOException
   */
  protected static void addTemplatePlugins(Id.ApplicationTemplate templateId, String jarName,
                                           Class<?> pluginClz, Class<?>... classes) throws IOException {
    getTestManager().addTemplatePlugins(templateId, jarName, pluginClz, classes);
  }

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
  protected static void addTemplatePluginJson(Id.ApplicationTemplate templateId, String fileName,
                                              String type, String name,
                                              String description, String className,
                                              PluginPropertyField... fields) throws IOException {
    getTestManager().addTemplatePluginJson(templateId, fileName, type, name, description, className, fields);
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
    deployDatasetModule(Constants.DEFAULT_NAMESPACE_ID, moduleName, datasetModule);
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
    return addDatasetInstance(Constants.DEFAULT_NAMESPACE_ID, datasetTypeName, datasetInstanceName, props);
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
    return addDatasetInstance(Constants.DEFAULT_NAMESPACE_ID, datasetTypeName, datasetInstanceName,
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
    return getDataset(Constants.DEFAULT_NAMESPACE_ID, datasetInstanceName);
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  protected final Connection getQueryClient(Id.Namespace namespace) throws Exception {
    return getTestManager().getQueryClient(namespace);
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  protected final Connection getQueryClient() throws Exception {
    return getQueryClient(Constants.DEFAULT_NAMESPACE_ID);
  }

  /**
   * Returns a {@link StreamManager} for the specified stream in the default namespace
   *
   * @param streamName the specified stream
   * @return {@link StreamManager} for the specified stream in the default namespace
   */
  protected final StreamManager getStreamManager(String streamName) throws Exception {
    return getStreamManager(Constants.DEFAULT_NAMESPACE_ID, streamName);
  }

  /**
   * Returns a {@link StreamManager} for the specified stream in the specified namespace
   *
   * @param streamName the specified stream
   * @return {@link StreamManager} for the specified stream in the specified namespace
   */
  protected final StreamManager getStreamManager(Id.Namespace namespace, String streamName) throws Exception {
    return getTestManager().getStreamManager(Id.Stream.from(namespace, streamName));
  }
}

