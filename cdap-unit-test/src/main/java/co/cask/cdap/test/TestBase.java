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
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.StickyEndpointStrategy;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.LocalStreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamFileJanitorService;
import co.cask.cdap.data.stream.service.StreamFileWriterSizeManager;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data.stream.service.StreamServiceModule;
import co.cask.cdap.data.stream.service.StreamWriterSizeManager;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.executor.ExploreExecutorService;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.gateway.handlers.AppFabricHttpHandler;
import co.cask.cdap.gateway.handlers.ServiceHttpHandler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.metrics.query.MetricsQueryService;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.DefaultApplicationManager;
import co.cask.cdap.test.internal.DefaultId;
import co.cask.cdap.test.internal.DefaultProcedureClient;
import co.cask.cdap.test.internal.DefaultStreamWriter;
import co.cask.cdap.test.internal.ProcedureClientFactory;
import co.cask.cdap.test.internal.StreamWriterFactory;
import co.cask.cdap.test.internal.TestMetricsCollectionService;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
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
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

/**
 * Base class to inherit from, provides testing functionality for {@link Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 */
public class TestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static Injector injector;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static LogAppenderInitializer logAppenderInitializer;
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
  private static StreamWriterSizeManager sizeManager;

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * {@link co.cask.cdap.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                 File...bundleEmbeddedJars) {
    
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");

    try {
      Object appInstance = applicationClz.newInstance();
      ApplicationSpecification appSpec;

      if (appInstance instanceof Application) {
        Application app = (Application) appInstance;
        DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
        app.configure(configurer, new ApplicationContext());
        appSpec = configurer.createSpecification();
      } else {
        throw new IllegalArgumentException("Application class does not represent application: "
                                             + applicationClz.getName());
      }

      Location deployedJar = appFabricClient.deployApplication(appSpec.getName(), applicationClz, bundleEmbeddedJars);

      return
        injector.getInstance(ApplicationManagerFactory.class).create(DefaultId.NAMESPACE.getId(), appSpec.getName(),
                                                                     deployedJar, appSpec);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Clear the state of app fabric, by removing all deployed applications, Datasets and Streams.
   * This method could be called between two unit tests, to make them independent.
   */
  protected void clear() {
    try {
      appFabricClient.reset();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.resetAll();
    }
  }

  @BeforeClass
  public static void init() throws Exception {
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
      new StreamServiceModule() {
        @Override
        protected void configure() {
          super.configure();
          bind(StreamHandler.class).in(Scopes.SINGLETON);
          bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);
          bind(StreamWriterSizeManager.class).to(StreamFileWriterSizeManager.class).in(Scopes.SINGLETON);
          bind(int.class).annotatedWith(Names.named(Constants.Stream.CONTAINER_INSTANCE_ID)).toInstance(0);
          expose(StreamWriterSizeManager.class);
          expose(StreamHandler.class);
        }
      },
      new TestMetricsClientModule(),
      new MetricsHandlerModule(),
      new LoggingModules().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(ApplicationManager.class, DefaultApplicationManager.class)
                    .build(ApplicationManagerFactory.class));
          install(new FactoryModuleBuilder()
                    .implement(StreamWriter.class, DefaultStreamWriter.class)
                    .build(StreamWriterFactory.class));
          install(new FactoryModuleBuilder()
                    .implement(ProcedureClient.class, DefaultProcedureClient.class)
                    .build(ProcedureClientFactory.class));
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
    sizeManager = injector.getInstance(StreamWriterSizeManager.class);
    sizeManager.startAndWait();
    AppFabricHttpHandler httpHandler = injector.getInstance(AppFabricHttpHandler.class);
    ServiceHttpHandler serviceHttpHandler = injector.getInstance(ServiceHttpHandler.class);
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    appFabricClient = new AppFabricClient(httpHandler, serviceHttpHandler, locationFactory);
    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    datasetFramework =
      new NamespacedDatasetFramework(dsFramework,
                                     new DefaultDatasetNamespace(cConf,  Namespace.USER));
    schedulerService = injector.getInstance(SchedulerService.class);
    schedulerService.startAndWait();
    discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();
    exploreClient = injector.getInstance(ExploreClient.class);
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
  }

  private static Module createDataFabricModule(final CConfiguration cConf) {
    return Modules.override(new DataFabricModules().getInMemoryModules(), new StreamAdminModules().getInMemoryModules())
      .with(new AbstractModule() {

        @Override
        protected void configure() {
          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
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
    sizeManager.stopAndWait();
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

  private static final class TestMetricsClientModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(MetricsCollectionService.class).to(TestMetricsCollectionService.class).in(Scopes.SINGLETON);
    }
  }

  /**
   * Deploys {@link DatasetModule}.
   * @param moduleName name of the module
   * @param datasetModule module class
   * @throws Exception
   */
  @Beta
  protected final void deployDatasetModule(String moduleName, Class<? extends DatasetModule> datasetModule)
    throws Exception {
    datasetFramework.addModule(moduleName, datasetModule.newInstance());
  }


  /**
   * Adds an instance of a dataset.
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   */
  @Beta
  protected final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                       String datasetInstanceName,
                                                       DatasetProperties props) throws Exception {

    datasetFramework.addInstance(datasetTypeName, datasetInstanceName, props);
    return datasetFramework.getAdmin(datasetInstanceName, null);
  }

  /**
   * Adds an instance of dataset.
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param <T> type of the dataset admin
   */
  @Beta
  protected final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                                String datasetInstanceName) throws Exception {

    datasetFramework.addInstance(datasetTypeName, datasetInstanceName, DatasetProperties.EMPTY);
    return datasetFramework.getAdmin(datasetInstanceName, null);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   * @param datasetInstanceName - instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  @Beta
  protected final <T> DataSetManager<T> getDataset(String datasetInstanceName)
    throws Exception {
    @SuppressWarnings("unchecked")
    final T dataSet = (T) datasetFramework.getDataset(datasetInstanceName, new HashMap<String, String>(), null);
    try {
      TransactionAware txAwareDataset = (TransactionAware) dataSet;
      final TransactionContext txContext =
        new TransactionContext(txSystemClient, Lists.newArrayList(txAwareDataset));
      txContext.start();
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            txContext.finish();
            txContext.start();
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
  protected final Connection getQueryClient() throws Exception {

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

    String connectString = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, host, port);

    return DriverManager.getConnection(connectString);
  }
}
