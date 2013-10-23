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
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.metadata.MetaDataTable;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
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
import java.util.concurrent.TimeUnit;

/**
 *
 */
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
    configuration = injector.getInstance(CConfiguration.class);

    // clean up data
    MetaDataTable mds = injector.getInstance(MetaDataTable.class);
    for (String account : mds.listAccounts(new OperationContext(DefaultId.DEFAULT_ACCOUNT_ID))) {
      mds.clear(new OperationContext(account), account, null);
    }
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
      ArchiveId id = server.init(token, new ArchiveInfo(DefaultId.ACCOUNT.getId(), "", deployedJar.getName()));

      // Upload the jar file to remote location.
      BufferFileInputStream is =
        new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024);
      try {
        while (true) {
          byte[] toSubmit = is.read();
          if (toSubmit.length == 0) {
            break;
          }
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
        while (status == 3) {
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

    ProgramId flowId = new ProgramId("account1", "application1", "WordCountFlow");
    String flowDefJson = server.getSpecification(flowId);
    Assert.assertNotNull(flowDefJson);

    // NOTE: After we migarted to specification - there is no point in testing as it's already
    // tested. But, will keep the minimal test for now.
  }

  @Test
  public void testGetFlowHistory() throws Exception {
    Store store = sFactory.create();
    // record finished flow
    Id.Program programId = new Id.Program(new Id.Application(new Id.Account("accountFlowHistoryTest1"),
                                                             "applicationFlowHistoryTest1"), "flowFlowHistoryTest1");

    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    store.setStart(programId, "run1", now - 1000);
    store.setStop(programId, "run1", now - 500, "FAILED");

    ProgramId flowId = new ProgramId("accountFlowHistoryTest1", "applicationFlowHistoryTest1",
                                               "flowFlowHistoryTest1");
    List<ProgramRunRecord> history = server.getHistory(flowId, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    Assert.assertEquals(1, history.size());
    ProgramRunRecord record = history.get(0);
    Assert.assertEquals(now - 1000, record.getStartTime());
    Assert.assertEquals(now - 500, record.getEndTime());
    Assert.assertEquals("FAILED", record.getEndStatus());
  }

  @Test
  public void testGetOldFlowHistoryDelete() throws Exception {
    Store store = sFactory.create();
    // record finished flow
    Id.Program programId = new Id.Program(new Id.Application(new Id.Account("accountOldFlowHistoryDelete"),
                                                             "applicationOldFlowHistoryTest1"), "flowOldHistoryDelete");

    // Register stop and start in the past beyond RUN_HISTORY_KEEP_DAYS from now.
    long timeBeyondHistory  =  TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                - Constants.DEFAULT_RUN_HISTORY_KEEP_DAYS * 24 * 60 * 60 - 1000;
    store.setStart(programId, "run1", timeBeyondHistory);
    store.setStop(programId, "run1", timeBeyondHistory, "STOPPED");

    ProgramId flowId = new ProgramId("accountOldFlowHistoryDelete", "applicationOldFlowHistoryTest1",
                                     "flowOldHistoryDelete");
    List<ProgramRunRecord> history = server.getHistory(flowId, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    //Should not get the history since we don't keep any history beyond MAX_HISTORY_KEEP_DAYS_IN_MILLIS
    Assert.assertEquals(0, history.size());
  }


  @Test
  public void testDeployApp() throws Exception {
    // Deploy and application.
    TestHelper.deployApplication(ToyApp.class, "ToyApp.jar");

    // Start a simple Jetty Server to simulate remote http server.
    Server jServer = new Server(configuration.getInt(Constants.AppFabric.REST_PORT, 10007));
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
    } finally {
      jServer.stop();
    }
  }
}
