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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
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
import co.cask.cdap.data.stream.service.StreamFetchHandlerV2;
import co.cask.cdap.data.stream.service.StreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data.stream.service.StreamHandlerV2;
import co.cask.cdap.data.stream.service.StreamWriterSizeCollector;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
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
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.gateway.handlers.ServiceHttpHandler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.List;

/**
 * Base class to inherit from, provides testing functionality for {@link Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 */
public class TestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static int startCount;
  private static Injector injector;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static AppFabricClient appFabricClient;
  private static SchedulerService schedulerService;
  private static DatasetFramework datasetFramework;
  private static TransactionSystemClient txSystemClient;
  private static DiscoveryServiceClient discoveryClient;
  private static ExploreExecutorService exploreExecutorService;
  private static ExploreClient exploreClient;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static TransactionManager txService;
  private static StreamCoordinatorClient streamCoordinatorClient;

  // This list is to record ApplicationManager create inside @Test method
  private static final List<ApplicationManager> applicationManagers = Lists.newArrayList();

  private static TestManager testManager;

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

  @BeforeClass
  public static void init() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    File localDataDir = tmpFolder.newFolder();
    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    cConf.set(MetricsConstants.ConfigKeys.SERVER_PORT, Integer.toString(Networks.getRandomPort()));

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    cConf.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    cConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, true);
    cConf.setBoolean(Constants.Explore.START_ON_DEMAND, true);
    cConf.set(Constants.Explore.LOCAL_DATA_DIR,
              tmpFolder.newFolder("hive").getAbsolutePath());

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
      binDir.mkdir();

      copyTempFile("hadoop.dll", tmpDir);
      copyTempFile("winutils.exe", binDir);
      System.setProperty("hadoop.home.dir", tmpDir.getAbsolutePath());
      System.load(new File(tmpDir, "hadoop.dll").getAbsolutePath());
    }

    injector = Guice.createInjector(
      createDataFabricModule(cConf),
      new DataSetsModules().getLocalModule(),
      new DataSetServiceModules().getInMemoryModule(),
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new AuthModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ServiceStoreModules().getInMemoryModule(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(StreamHandlerV2.class).in(Scopes.SINGLETON);
          bind(StreamFetchHandlerV2.class).in(Scopes.SINGLETON);
          bind(StreamHandler.class).in(Scopes.SINGLETON);
          bind(StreamFetchHandler.class).in(Scopes.SINGLETON);
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
    AppFabricHttpHandler httpHandler = injector.getInstance(AppFabricHttpHandler.class);
    ServiceHttpHandler serviceHttpHandler = injector.getInstance(ServiceHttpHandler.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    appFabricClient = new AppFabricClient(httpHandler, serviceHttpHandler, locationFactory);
    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    datasetFramework = new NamespacedDatasetFramework(dsFramework, new DefaultDatasetNamespace(cConf));
    schedulerService = injector.getInstance(SchedulerService.class);
    schedulerService.startAndWait();
    discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();
    exploreClient = injector.getInstance(ExploreClient.class);
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);
    streamCoordinatorClient.startAndWait();
    testManager = new UnitTestManager(appFabricClient, datasetFramework, txSystemClient, discoveryClient);
    // we use MetricStore directly, until RuntimeStats API changes
    RuntimeStats.metricStore = injector.getInstance(MetricStore.class);
  }

  private static Module createDataFabricModule(final CConfiguration cConf) {
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

  private static void copyTempFile (String infileName, File outDir) {
    InputStream in = null;
    FileOutputStream out = null;
    try {
      in = TestBase.class.getClassLoader().getResourceAsStream(infileName);
      out = new FileOutputStream(new File(outDir, infileName)); // localized within container, so it get cleaned.
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @AfterClass
  public static final void finish() {
    if (--startCount != 0) {
      return;
    }

    streamCoordinatorClient.stopAndWait();
    metricsQueryService.stopAndWait();
    metricsCollectionService.startAndWait();
    schedulerService.stopAndWait();
    try {
      exploreClient.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    exploreExecutorService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
  }

  private static void cleanDir(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.isFile()) {
        file.delete();
      } else {
        cleanDir(file);
      }
    }
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * {@link co.cask.cdap.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Id.Namespace namespace,
                                                        Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    ApplicationManager manager = getTestManager().deployApplication(namespace, applicationClz, bundleEmbeddedJars);
    applicationManagers.add(manager);
    return manager;
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * {@link co.cask.cdap.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  protected static ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                        File... bundleEmbeddedJars) {
    return deployApplication(Constants.DEFAULT_NAMESPACE_ID, applicationClz, bundleEmbeddedJars);
  }

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   *
   */
  protected void clear() {
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
   * @param namespace namespace to deploy the module to
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
  protected static <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                                 String datasetTypeName,
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
  protected static <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                                 String datasetTypeName,
                                                                 String datasetInstanceName) throws Exception {
    return addDatasetInstance(namespace, datasetTypeName, datasetInstanceName, DatasetProperties.EMPTY);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>.
   *
   * @param namespace the namespace of the dataset
   * @param datasetInstanceName instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  protected static <T> DataSetManager<T> getDataset(Id.Namespace namespace,
                                                    String datasetInstanceName) throws Exception {
    return getTestManager().getDataset(namespace, datasetInstanceName);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>.
   *
   * @param datasetInstanceName instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  protected static <T> DataSetManager<T> getDataset(String datasetInstanceName) throws Exception {
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
    return getTestManager().getQueryClient(Constants.DEFAULT_NAMESPACE_ID);
  }
}
