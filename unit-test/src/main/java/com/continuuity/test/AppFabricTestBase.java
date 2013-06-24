package com.continuuity.test;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.ApplicationManagerFactory;
import com.continuuity.test.app.DefaultApplicationManager;
import com.continuuity.test.app.DefaultId;
import com.continuuity.test.app.DefaultProcedureClient;
import com.continuuity.test.app.DefaultStreamWriter;
import com.continuuity.test.app.ProcedureClient;
import com.continuuity.test.app.ProcedureClientFactory;
import com.continuuity.test.app.StreamWriter;
import com.continuuity.test.app.StreamWriterFactory;
import com.continuuity.test.app.TestHelper;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Base class to inherit from that provides testing functionality for {@link com.continuuity.api.Application}.
 */
public class AppFabricTestBase {

  private static File testAppDir;
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;

  /**
   * Deploys an {@link com.continuuity.api.Application}. The {@link com.continuuity.api.flow.Flow Flows} and
   * {@link com.continuuity.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    try {

      ApplicationSpecification appSpec = applicationClz.newInstance().configure();

      Location deployedJar = TestHelper.deployApplication(appFabricServer, locationFactory, DefaultId.ACCOUNT,
                                                          TestHelper.DUMMY_AUTH_TOKEN, "", appSpec.getName(),
                                                          applicationClz);

      return
        injector.getInstance(ApplicationManagerFactory.class).create(TestHelper.DUMMY_AUTH_TOKEN,
                                                                     DefaultId.ACCOUNT.getId(), appSpec.getName(),
                                                                     appFabricServer, deployedJar, appSpec);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected void clearAppFabric() {
    try {
      appFabricServer.reset(TestHelper.DUMMY_AUTH_TOKEN, DefaultId.ACCOUNT.getId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @BeforeClass
  public static final void init() {
    testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    File tmpDir = new File(testAppDir, "tmp");

    outputDir.mkdirs();
    tmpDir.mkdirs();

    CConfiguration configuration = CConfiguration.create();
    configuration.set("app.output.dir", outputDir.getAbsolutePath());
    configuration.set("app.tmp.dir", tmpDir.getAbsolutePath());

    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                    new ConfigModule(configuration),
                                    new IOModule(),
                                    new LocationRuntimeModule().getInMemoryModules(),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new AppFabricServiceRuntimeModule().getInMemoryModules(),
                                    new ProgramRunnerRuntimeModule().getInMemoryModules(),
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
                                    });
    appFabricServer = injector.getInstance(AppFabricService.Iface.class);
    locationFactory = injector.getInstance(LocationFactory.class);
  }

  @AfterClass
  public static final void finish() {
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
}

