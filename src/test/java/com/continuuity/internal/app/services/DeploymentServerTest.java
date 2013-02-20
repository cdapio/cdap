/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.DumbProgrammerApp;
import com.continuuity.TestHelper;
import com.continuuity.ToyApp;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentService;
import com.continuuity.app.services.DeploymentServiceException;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.factories.PassportAuthorizationFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.app.services.DeploymentServerFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DeploymentServerTest {
  private static DeploymentService.Iface server;
  private static LocationFactory lf;

  private static class SimpleDeploymentServerModule extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {
      bind(DeploymentServerFactory.class).to(SimpleDeploymentServerFactory.class);
      bind(LocationFactory.class).to(LocalLocationFactory.class);
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);
      bind(StoreFactory.class).to(MDSStoreFactory.class);
      bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
      bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    }
  }

  @BeforeClass
  public static void before() throws Exception {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new SimpleDeploymentServerModule());
    DeploymentServerFactory factory = injector.getInstance(DeploymentServerFactory.class);

    CConfiguration configuration = CConfiguration.create();
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");

    // Create the server.
    server = factory.create(configuration);

    // Create location factory.
    lf = injector.getInstance(LocationFactory.class);
  }

  @Test
  public void testUploadToDeploymentServer() throws Exception {
    // Create a local jar - simulate creation of application archive.
    Location deployedJar = lf.create(
      JarFinder.getJar(ToyApp.class, TestHelper.getManifestWithMainClass(ToyApp.class))
    );
    deployedJar.deleteOnExit();

    // Call init to get a session identifier - yes, the name needs to be changed.
    AuthToken token = new AuthToken("12345");
    ResourceIdentifier id = server.init(token, new ResourceInfo("demo","",deployedJar.getName(), 123455, 45343));

    // Upload the jar file to remote location.
    BufferFileInputStream is =
      new BufferFileInputStream(deployedJar.getInputStream(), 100*1024);
    try {
      while(true) {
        byte[] toSubmit = is.read();
        if(toSubmit.length==0) break;
        server.chunk(token, id, ByteBuffer.wrap(toSubmit));
        DeploymentStatus status = server.status(token, id);
        Assert.assertEquals(2, status.getOverall());
      }
    } finally {
      is.close();
    }

    server.deploy(token, id);
    int status = server.status(token, id).getOverall();
    while(status == 3) {
      status = server.status(token, id).getOverall();
      Thread.sleep(100);
    }
    Assert.assertEquals(5, status); // Deployed successfully.
  }

  @Test
  public void testDumbProgrammerFailingApp() throws Exception {
    // Create a local jar - simulate creation of application archive.
    Location deployedJar = lf.create(
      JarFinder.getJar(DumbProgrammerApp.class, TestHelper.getManifestWithMainClass(DumbProgrammerApp.class))
    );
    deployedJar.deleteOnExit();

    // Call init to get a session identifier - yes, the name needs to be changed.
    AuthToken token = new AuthToken("12345");
    ResourceIdentifier id = server.init(token, new ResourceInfo("demo","",deployedJar.getName(), 123455, 45343));

    // Upload the jar file to remote location.
    BufferFileInputStream is =
      new BufferFileInputStream(deployedJar.getInputStream(), 100*1024);
    try {
      while(true) {
        byte[] toSubmit = is.read();
        if(toSubmit.length==0) break;
        server.chunk(token, id, ByteBuffer.wrap(toSubmit));
        DeploymentStatus status = server.status(token, id);
        Assert.assertEquals(2, status.getOverall());
      }
    } finally {
      is.close();
    }

    // Now start the verification.
    int status = 0;
    try {
      server.deploy(token, id);
      status = server.status(token, id).getOverall();
      while(status == 3) {
        status = server.status(token, id).getOverall();
        Thread.sleep(100);
      }
    } catch (DeploymentServiceException e) {
      // What will you do ?
    } catch (TException e) {
      // What will you do ?
    } catch (InterruptedException e) {
      // What will you do ?
    }
    status = server.status(token, id).getOverall();
    Assert.assertEquals(4, status); // Deployed successfully.
  }
}
