package com.continuuity.test;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.services.AppFabricServer;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.continuuity.metrics.query.MetricsQueryService;
import com.continuuity.test.internal.ApplicationManagerFactory;
import com.continuuity.test.internal.DefaultApplicationManager;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.DefaultProcedureClient;
import com.continuuity.test.internal.DefaultStreamWriter;
import com.continuuity.test.internal.ProcedureClientFactory;
import com.continuuity.test.internal.StreamWriterFactory;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.test.internal.TestMetricsCollectionService;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Base class to inherit from, provides testing functionality for {@link com.continuuity.api.Application}.
 */
public class ReactorTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static File testAppDir;
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static MetricsQueryService metricsQueryService;
  private static MetricsCollectionService metricsCollectionService;
  private static LogAppenderInitializer logAppenderInitializer;

  /**
   * Deploys an {@link com.continuuity.api.Application}. The {@link com.continuuity.api.flow.Flow Flows} and
   * {@link com.continuuity.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link com.continuuity.test.ApplicationManager} to manage the deployed application.
   */
  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    try {

      ApplicationSpecification appSpec =
        Specifications.from(applicationClz.newInstance().configure());

      Location deployedJar = TestHelper.deployApplication(appFabricServer, locationFactory, DefaultId.ACCOUNT,
                                                          TestHelper.DUMMY_AUTH_TOKEN, null, appSpec.getName(),
                                                          applicationClz);

      return
        injector.getInstance(ApplicationManagerFactory.class).create(TestHelper.DUMMY_AUTH_TOKEN,
                                                                     DefaultId.ACCOUNT.getId(), appSpec.getName(),
                                                                     appFabricServer, deployedJar, appSpec);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected void clear() {
    try {
      appFabricServer.reset(TestHelper.DUMMY_AUTH_TOKEN, DefaultId.ACCOUNT.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @BeforeClass
  public static final void init() throws IOException {
    testAppDir = tmpFolder.newFolder();

    File outputDir = new File(testAppDir, "app");
    File tmpDir = new File(testAppDir, "tmp");

    outputDir.mkdirs();
    tmpDir.mkdirs();

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.AppFabric.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
    configuration.set(MetricsConstants.ConfigKeys.SERVER_PORT, Integer.toString(Networks.getRandomPort()));
    configuration.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder("data").getAbsolutePath());

    // Windows specific requirements
    if (System.getProperty("os.name").startsWith("Windows")) {

      File binDir = new File(tmpDir, "bin");
      binDir.mkdir();

      copyTempFile("hadoop.dll", tmpDir);
      copyTempFile("winutils.exe", binDir);
      System.setProperty("hadoop.home.dir", tmpDir.getAbsolutePath());
      System.load(new File(tmpDir, "hadoop.dll").getAbsolutePath());
    }

    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                    new ConfigModule(configuration),
                                    new IOModule(),
                                    new AuthModule(),
                                    new LocationRuntimeModule().getInMemoryModules(),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new AppFabricServiceRuntimeModule().getInMemoryModules(),
                                    new ProgramRunnerRuntimeModule().getInMemoryModules(),
                                    new TestMetricsClientModule(),
                                    new MetricsHandlerModule(),
                                    new LoggingModules().getInMemoryModules(),
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
    appFabricServer = injector.getInstance(AppFabricServer.class).getService();
    locationFactory = injector.getInstance(LocationFactory.class);
    metricsQueryService = injector.getInstance(MetricsQueryService.class);
    metricsQueryService.startAndWait();
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();
    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();
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
}

