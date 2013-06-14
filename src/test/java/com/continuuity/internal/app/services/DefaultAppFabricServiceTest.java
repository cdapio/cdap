/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.DumbProgrammerApp;
import com.continuuity.ToyApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.app.services.legacy.ConnectionDefinition;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.continuuity.test.app.DefaultId;
import com.continuuity.test.app.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.gson.Gson;
import com.google.inject.Injector;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class DefaultAppFabricServiceTest {
  private static AppFabricService.Iface server;
  private static LocationFactory lf;
  private static StoreFactory sFactory;
  private static CConfiguration configuration;

  @BeforeClass
  public static void before() throws Exception {
    final Injector injector = TestHelper.getInjector();
    server = injector.getInstance(AppFabricService.Iface.class);
    // Create location factory.
    lf = injector.getInstance(LocationFactory.class);
    // Create store
    sFactory = injector.getInstance(StoreFactory.class);
  }

  @Test
  public void testUploadToDeploymentServer() throws Exception {
    TestHelper.deployApplication(ToyApp.class);
    TestHelper.deployApplication(WordCountApp.class);
  }

  @Test
  public void testDumbProgrammerFailingApp() throws Exception {
    // Create a local jar - simulate creation of application archive.
    Location deployedJar = lf.create(
      JarFinder.getJar(DumbProgrammerApp.class, TestHelper.getManifestWithMainClass(DumbProgrammerApp.class))
    );

    try {
      // Call init to get a session identifier - yes, the name needs to be changed.
      AuthToken token = new AuthToken("12345");
      ResourceIdentifier id = server.init(token, new ResourceInfo(DefaultId.ACCOUNT.getId(),"",deployedJar.getName(),
                                                                  123455, 45343));

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
    } finally {
      deployedJar.delete(true);
    }
  }

  @Test
  public void testGetFlowDefinition() throws Exception {
    Store store = sFactory.create();
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application appId = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(appId, spec, new LocalLocationFactory().create("/foo"));

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
    Store store = sFactory.create();
    // record finished flow
    Id.Program programId = new Id.Program(new Id.Application(new Id.Account("accountFlowHistoryTest1"),
                                                             "applicationFlowHistoryTest1"), "flowFlowHistoryTest1");
    store.setStart(programId, "run1", 20);
    store.setStop(programId, "run1", 29, "FAILED");

    FlowIdentifier flowId = new FlowIdentifier("accountFlowHistoryTest1", "applicationFlowHistoryTest1",
                                               "flowFlowHistoryTest1", 0);
    List<FlowRunRecord> history = server.getFlowHistory(flowId);
    Assert.assertEquals(1, history.size());
    FlowRunRecord record = history.get(0);
    Assert.assertEquals(20, record.getStartTime());
    Assert.assertEquals(29, record.getEndTime());
    Assert.assertEquals("FAILED", record.getEndStatus());
  }

  @Test
  public void testDeployAApp() throws Exception {
    // Deploy and application.
    TestHelper.deployApplication(ToyApp.class, "ToyApp.jar");

    // Start a simple Jetty Server to simulate remote http server.
    Server jServer = new Server(10007);
    try {
      jServer.setHandler(new AbstractHandler() {
        @Override
        public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
          throws IOException, ServletException {
          try {
            String archiveName = request.getHeader("X-Archive-Name");
            Assert.assertNotNull(archiveName);
            Assert.assertTrue(!archiveName.isEmpty());
            String token = request.getHeader("X-Continuuity-ApiKey");
            Assert.assertNotNull(token);
            Assert.assertTrue(!token.isEmpty());
            Assert.assertEquals("PUT", request.getMethod());
            Assert.assertTrue(request.getContentLength() != 0);
          } catch (Exception e) {
            response.setContentType("text/plain");
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().close();
            return;
          }
          response.setContentType("text/plain");
          response.setStatus(HttpServletResponse.SC_OK);
          response.getWriter().close();
          return;
        }
      });
      jServer.start();

      ResourceIdentifier id = new ResourceIdentifier(DefaultId.ACCOUNT.getId(), "ToyApp", "whatever", 1);
      // Now send in deploy request.
      try {
        server.promote(new AuthToken(TestHelper.DUMMY_AUTH_TOKEN), id, "localhost");
      } catch (AppFabricServiceException e) {
        Assert.assertTrue(false);
      } catch (TException e) {
        Assert.assertTrue(false);
      }
    } finally {
      jServer.stop();
    }
  }
}
