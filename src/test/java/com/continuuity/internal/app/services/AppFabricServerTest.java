/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.DumbProgrammerApp;
import com.continuuity.TestHelper;
import com.continuuity.ToyApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Status;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.app.store.Store;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.internal.app.services.legacy.ConnectionDefinition;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.app.services.DeploymentServerFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class AppFabricServerTest {
  private static AppFabricService.Iface server;
  private static LocationFactory lf;
  private static StoreFactory sFactory;
  private static CConfiguration configuration;

  private static class SimpleDeploymentServerModule extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {
      bind(DeploymentServerFactory.class).to(InMemoryAppFabricServerFactory.class);
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

    configuration = CConfiguration.create();
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");

    // Create the server.
    server = factory.create(configuration);

    // Create location factory.
    lf = injector.getInstance(LocationFactory.class);

    // Create store
    sFactory = injector.getInstance(StoreFactory.class);
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
        DeploymentStatus status = server.dstatus(token, id);
        Assert.assertEquals(2, status.getOverall());
      }
    } finally {
      is.close();
    }

    // Now start the verification.
    int status = 0;
    try {
      server.deploy(token, id);
      status = server.dstatus(token, id).getOverall();
      while(status == 3) {
        status = server.dstatus(token, id).getOverall();
        Thread.sleep(100);
      }
    } catch (AppFabricServiceException e) {
      // What will you do ?
    } catch (TException e) {
      // What will you do ?
    } catch (InterruptedException e) {
      // What will you do ?
    }
    status = server.dstatus(token, id).getOverall();
    Assert.assertEquals(4, status); // Deployed successfully.
  }

  @Test
  public void testGetFlowDefinition() throws Exception {
    Store store = sFactory.create(configuration);
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application appId = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(appId, spec);

    FlowIdentifier flowId = new FlowIdentifier("account1", "application1", "WordCountFlow", 0);
    String flowDefJson = server.getFlowDefinition(flowId);
    FlowDefinitionImpl flowDef = new Gson().fromJson(flowDefJson, FlowDefinitionImpl.class);

    Assert.assertEquals(3, flowDef.getFlowlets().size());
    Assert.assertEquals(1, flowDef.getFlowStreams().size());

    // checking connections (most important stuff)
    Assert.assertEquals(3, flowDef.getConnections().size());
    int[] connectionFound = new int[3];
    for (ConnectionDefinition conn : flowDef.getConnections()) {
      if (conn.getFrom().isFlowStream()) {
        connectionFound[0]++;
        Assert.assertEquals("text", conn.getFrom().getStream());
      } else {
        if ("Tokenizer".equals(conn.getFrom().getFlowlet())) {
          connectionFound[1]++;
          Assert.assertEquals("CountByField", conn.getTo().getFlowlet());
        } else if ("StreamSource".equals(conn.getFrom().getFlowlet())) {
          connectionFound[2]++;
          Assert.assertEquals("Tokenizer", conn.getTo().getFlowlet());
        }
      }
    }
    Assert.assertArrayEquals(new int[]{1, 1, 1}, connectionFound);
  }

  @Test
  public void testGetFlowHistory() throws Exception {
    Store store = sFactory.create(configuration);
    // record finished flow
    Id.Program programId = new Id.Program(new Id.Application(new Id.Account("account1"), "application1"), "flow1");
    store.setStart(programId, "run1", 20);
    store.setEnd(programId, "run1", 29, Status.FAILED);

    FlowIdentifier flowId = new FlowIdentifier("account1", "application1", "flow1", 0);
    List<FlowRunRecord> history = server.getFlowHistory(flowId);
    Assert.assertEquals(1, history.size());
    FlowRunRecord record = history.get(0);
    Assert.assertEquals(20, record.getStartTime());
    Assert.assertEquals(29, record.getEndTime());
    Assert.assertEquals(Status.FAILED, Status.valueOf(record.getEndStatus()));
  }
}
