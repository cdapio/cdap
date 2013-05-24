/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.app.DefaultId;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.jar.Manifest;

/**
 * This is a test helper for our internal test.
 * <p>
 *   <i>Note: please don't include this in the developer test</i>
 * </p>
 */
public class TestHelper {
  public static CConfiguration configuration;
  private static Injector injector;

  static {
    TempFolder tempFolder = new TempFolder();
    configuration = CConfiguration.create();
    configuration.set("app.output.dir", tempFolder.newFolder("app").getAbsolutePath());
    configuration.set("app.tmp.dir", tempFolder.newFolder("temp").getAbsolutePath());
    final Configuration hConf = new Configuration();
    // we use it to make mapreduce framework to use our fixed LocalJobRunner in lcoal mode
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();

    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                    new BigMamaModule(TestHelper.configuration),
                                    new Module() {
                                      @Override
                                      public void configure(Binder binder) {
                                        binder.bind(Configuration.class).toInstance(hConf);
                                      }
                                    });
  }

  public static Injector getInjector() {
    return injector;
  }

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getName());
    return manifest;
  }

  /**
   * @return Returns an instance of {@link LocalManager}
   */
  public static Manager<Location, ApplicationWithPrograms> getLocalManager() {
    ManagerFactory factory = injector.getInstance(ManagerFactory.class);
    return (Manager<Location, ApplicationWithPrograms>)factory.create();
  }

  public static void deployApplication(Class<? extends Application> application) throws Exception {
    deployApplication(application, "app-" + System.currentTimeMillis()/1000 + ".jar");
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Class<? extends Application> appClass) throws Exception {
    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(appClass, TestHelper.getManifestWithMainClass(appClass))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager().deploy(DefaultId.ACCOUNT, deployedJar);
    return (ApplicationWithPrograms) p.get();
  }

  /**
   *
   */
  public static void deployApplication(Class<? extends Application> application, String fileName) throws Exception {
    AppFabricService.Iface server;

    server = injector.getInstance(AppFabricService.Iface.class);

    // Create location factory.
    LocationFactory lf = injector.getInstance(LocationFactory.class);

    // Create a local jar - simulate creation of application archive.
    Location deployedJar = lf.create(
      JarFinder.getJar(application, TestHelper.getManifestWithMainClass(application))
    );
    deployedJar.deleteOnExit();

    // Call init to get a session identifier - yes, the name needs to be changed.
    AuthToken token = new AuthToken("12345");
    ResourceIdentifier id = server.init(token, new ResourceInfo("developer","", fileName, 123455, 45343));

    // Upload the jar file to remote location.
    BufferFileInputStream is =
      new BufferFileInputStream(deployedJar.getInputStream(), 100*1024);
    try {
      while(true) {
        byte[] toSubmit = is.read();
        if(toSubmit.length==0) break;
        server.chunk(token, id, ByteBuffer.wrap(toSubmit));
        DeploymentStatus status = server.dstatus(token, id);
        Assert.assertEquals(2, status.getOverall());
      }
    } finally {
      is.close();
    }

    server.deploy(token, id);
    int status = server.dstatus(token, id).getOverall();
    while(status == 3) {
      status = server.dstatus(token, id).getOverall();
      Thread.sleep(100);
    }
    Assert.assertEquals(5, status); // Deployed successfully.
  }
}
