package com.continuuity.test;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.app.Application;
import com.continuuity.api.app.ApplicationContext;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.DefaultAppConfigurer;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.guice.ServiceStoreModules;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.StickyEndpointStrategy;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.common.utils.OSDetector;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.data.runtime.LocationStreamFileWriterFactory;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.service.LocalStreamFileJanitorService;
import com.continuuity.data.stream.service.StreamFileJanitorService;
import com.continuuity.data.stream.service.StreamHandler;
import com.continuuity.data.stream.service.StreamServiceModule;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.NamespacedDatasetFramework;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.data2.transaction.stream.StreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import com.continuuity.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import com.continuuity.explore.executor.ExploreExecutorService;
import com.continuuity.explore.guice.ExploreRuntimeModule;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.ApplicationManagerFactory;
import com.continuuity.test.internal.DefaultApplicationManager;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.DefaultProcedureClient;
import com.continuuity.test.internal.DefaultStreamWriter;
import com.continuuity.test.internal.ProcedureClientFactory;
import com.continuuity.test.internal.StreamWriterFactory;
import com.continuuity.test.internal.TestMetricsCollectionService;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.util.Modules;
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

/**
 * Base class to inherit from, provides testing functionality for {@link com.continuuity.api.Application}.
 */
public class ReactorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static File testAppDir;
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static LogAppenderInitializer logAppenderInitializer;
  private static AppFabricHttpHandler httpHandler;
  private static SchedulerService schedulerService;
  private static DatasetService datasetService;
  private static DatasetFramework datasetFramework;
  private static DiscoveryServiceClient discoveryClient;
  private static ExploreExecutorService exploreExecutorService;

  /**
   * Deploys an {@link com.continuuity.api.Application}. The {@link com.continuuity.api.flow.Flow Flows} and
   * {@link com.continuuity.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link com.continuuity.test.ApplicationManager} to manage the deployed application.
   */
  protected ApplicationManager deployApplication(Class<?> applicationClz,
                                                 File...bundleEmbeddedJars) {
    
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");

    try {
      Object appInstance = applicationClz.newInstance();
      ApplicationSpecification appSpec;

      if (appInstance instanceof Application) {
        Application app = (Application) appInstance;
        DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
        app.configure(configurer, new ApplicationContext());
        appSpec = configurer.createApplicationSpec();
      } else if (appInstance instanceof com.continuuity.api.Application) {
        appSpec = Specifications.from(((com.continuuity.api.Application) appInstance).configure());
      } else {
        throw new IllegalArgumentException("Application class does not represent application: "
                                             + applicationClz.getName());
      }

      Location deployedJar = AppFabricTestHelper.deployApplication(httpHandler, locationFactory, appSpec.getName(),
                                                                   applicationClz, bundleEmbeddedJars);

      return
        injector.getInstance(ApplicationManagerFactory.class).create(DefaultId.ACCOUNT.getId(), appSpec.getName(),
                                                                     deployedJar, appSpec);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected void clear() {
    try {
      AppFabricTestHelper.reset(httpHandler);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @BeforeClass
  public static void init() throws Exception {
    testAppDir = tmpFolder.newFolder();

    File appDir = new File(testAppDir, "app");
    File datasetDir = new File(testAppDir, "dataset");
    File tmpDir = new File(testAppDir, "tmp");

    appDir.mkdirs();
    datasetDir.mkdirs();
    tmpDir.mkdirs();

    CConfiguration configuration = CConfiguration.create();

    configuration.set(Constants.AppFabric.OUTPUT_DIR, appDir.getAbsolutePath());
    configuration.set(Constants.AppFabric.TEMP_DIR, tmpDir.getAbsolutePath());
    configuration.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());
    configuration.set(Constants.Dataset.Manager.ADDRESS, "localhost");
    configuration.set(MetricsConstants.ConfigKeys.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
    configuration.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder("data").getAbsolutePath());
    configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
    configuration.setBoolean(Constants.Explore.CFG_EXPLORE_ENABLED, true);
    configuration.set(Constants.Explore.CFG_LOCAL_DATA_DIR,
                      tmpFolder.newFolder("hive").getAbsolutePath());

    // Windows specific requirements
    if (OSDetector.isWindows()) {

      File binDir = new File(tmpDir, "bin");
      binDir.mkdir();

      copyTempFile("hadoop.dll", tmpDir);
      copyTempFile("winutils.exe", binDir);
      System.setProperty("hadoop.home.dir", tmpDir.getAbsolutePath());
      System.load(new File(tmpDir, "hadoop.dll").getAbsolutePath());
    }

    injector = Guice.createInjector(
      createDataFabricModule(configuration),
      new DataSetsModules().getInMemoryModule(),
      new DataSetServiceModules().getInMemoryModule(),
      new ConfigModule(configuration),
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
          expose(StreamHandler.class);
        }
      },
      new TestMetricsClientModule(),
      new MetricsHandlerModule(),
      new LoggingModules().getInMemoryModules(),
      new ExploreRuntimeModule().getInMemoryModules(),
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
        }
      }
    );
    injector.getInstance(InMemoryTransactionManager.class).startAndWait();
    locationFactory = injector.getInstance(LocationFactory.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    metricsQueryService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();
    httpHandler = injector.getInstance(AppFabricHttpHandler.class);
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    datasetFramework =
      new NamespacedDatasetFramework(dsFramework,
                                     new ReactorDatasetNamespace(configuration,  DataSetAccessor.Namespace.USER));
    schedulerService = injector.getInstance(SchedulerService.class);
    schedulerService.startAndWait();
    discoveryClient = injector.getInstance(DiscoveryServiceClient.class);
    exploreExecutorService = injector.getInstance(ExploreExecutorService.class);
    exploreExecutorService.startAndWait();
  }

  private static Module createDataFabricModule(final CConfiguration cConf) {
    return Modules.override(new DataFabricModules().getInMemoryModules())
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
      in = ReactorTestBase.class.getClassLoader().getResourceAsStream(infileName);
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
    metricsQueryService.stopAndWait();
    metricsCollectionService.startAndWait();
    datasetService.stopAndWait();
    schedulerService.stopAndWait();
    exploreExecutorService.stopAndWait();
    logAppenderInitializer.close();
    cleanDir(testAppDir);
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
   * Adds instance of data set.
   * @param datasetTypeName dataset type name
   * @param datasetInstanceName instance name
   * @param props properties
   * @param <T> type of the dataset admin
   * @return
   * @throws Exception
   */
  @Beta
  protected final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                       String datasetInstanceName,
                                                       DatasetProperties props) throws Exception {

    datasetFramework.addInstance(datasetTypeName, datasetInstanceName, props);
    return datasetFramework.getAdmin(datasetInstanceName, null);
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  @Beta
  protected final Connection getQueryClient() throws Exception {

    // this makes sure the Explore JDBC driver is loaded
    Class.forName("com.continuuity.explore.jdbc.ExploreDriver");

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
