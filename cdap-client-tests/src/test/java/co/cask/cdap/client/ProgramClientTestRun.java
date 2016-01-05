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

package co.cask.cdap.client;

import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.BatchProgram;
import co.cask.cdap.proto.BatchProgramResult;
import co.cask.cdap.proto.BatchProgramStart;
import co.cask.cdap.proto.BatchProgramStatus;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link ProgramClient}.
 */
@Category(XSlowTests.class)
public class ProgramClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramClientTestRun.class);

  private ApplicationClient appClient;
  private ProgramClient programClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
  }

  @Test
  public void testBatchProgramCalls() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Application appId = Id.Application.from(namespace, FakeApp.NAME);
    BatchProgram flow = new BatchProgram(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
    BatchProgram service = new BatchProgram(FakeApp.NAME, ProgramType.SERVICE, PingService.NAME);
    BatchProgram missing = new BatchProgram(FakeApp.NAME, ProgramType.FLOW, "not" + FakeFlow.NAME);

    appClient.deploy(namespace, createAppJarFile(FakeApp.class));
    try {
      // make a batch call to start multiple programs, one of which does not exist
      List<BatchProgramStart> programStarts = ImmutableList.of(
        new BatchProgramStart(flow),
        new BatchProgramStart(service),
        new BatchProgramStart(missing)
      );
      List<BatchProgramResult> results = programClient.start(namespace, programStarts);
      // check that we got a 200 for programs that exist, and a 404 for the one that doesn't
      for (BatchProgramResult result : results) {
        if (missing.getProgramId().equals(result.getProgramId())) {
          Assert.assertEquals(404, result.getStatusCode());
        } else {
          Assert.assertEquals(200, result.getStatusCode());
        }
      }

      // wait for all programs to be in RUNNING status
      programClient.waitForStatus(
        Id.Program.from(namespace, flow.getAppId(), flow.getProgramType(), flow.getProgramId()),
        "RUNNING", 2, TimeUnit.MINUTES);
      programClient.waitForStatus(
        Id.Program.from(namespace, service.getAppId(), service.getProgramType(), service.getProgramId()),
        "RUNNING", 2, TimeUnit.MINUTES);

      // make a batch call for status of programs, one of which does not exist
      List<BatchProgram> programs = ImmutableList.of(flow, service, missing);
      List<BatchProgramStatus> statusList = programClient.getStatus(namespace, programs);
      // check status is running for programs that exist, and that we get a 404 for the one that doesn't
      for (BatchProgramStatus status : statusList) {
        if (missing.getProgramId().equals(status.getProgramId())) {
          Assert.assertEquals(404, status.getStatusCode());
        } else {
          Assert.assertEquals(200, status.getStatusCode());
          Assert.assertEquals("RUNNING", status.getStatus());
        }
      }

      // make a batch call to stop programs, one of which does not exist
      results = programClient.stop(namespace, programs);
      // check that we got a 200 for programs that exist, and a 404 for the one that doesn't
      for (BatchProgramResult result : results) {
        if (missing.getProgramId().equals(result.getProgramId())) {
          Assert.assertEquals(404, result.getStatusCode());
        } else {
          Assert.assertEquals(200, result.getStatusCode());
        }
      }

      // check programs are in stopped state
      programs = ImmutableList.of(flow, service);
      statusList = programClient.getStatus(namespace, programs);
      for (BatchProgramStatus status : statusList) {
        Assert.assertEquals(200, status.getStatusCode());
        Assert.assertEquals("Program = " + status.getProgramId(), "STOPPED", status.getStatus());
      }
    } finally {
      try {
        appClient.delete(appId);
      } catch (Exception e) {
        LOG.error("Error deleting app {} during test cleanup.", appId, e);
      }
    }
  }

  @Test
  public void testAll() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Application app = Id.Application.from(namespace, FakeApp.NAME);
    Id.Flow flow = Id.Flow.from(app, FakeFlow.NAME);
    Id.Flow.Flowlet flowlet = Id.Flow.Flowlet.from(flow, FakeFlow.FLOWLET_NAME);

    appClient.deploy(namespace, createAppJarFile(FakeApp.class));

    try {
      // start, scale, and stop flow
      verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(app, ProgramType.FLOW));

      LOG.info("Starting flow");
      programClient.start(flow);
      assertProgramRunning(programClient, flow);

      LOG.info("Getting flow history");
      programClient.getAllProgramRuns(flow, 0, Long.MAX_VALUE, Integer.MAX_VALUE);

      LOG.info("Scaling flowlet");
      Assert.assertEquals(1, programClient.getFlowletInstances(flowlet));
      programClient.setFlowletInstances(flowlet, 3);
      assertFlowletInstances(programClient, flowlet, 3);

      List<BatchProgram> statusRequest = new ArrayList<>();
      for (ProgramRecord programRecord : appClient.listPrograms(app)) {
        statusRequest.add(BatchProgram.from(programRecord));
      }
      List<BatchProgramStatus> statuses = programClient.getStatus(namespace, statusRequest);
      for (BatchProgramStatus status : statuses) {
        if (status.getProgramType() == ProgramType.FLOW && status.getProgramId().equals(FakeFlow.NAME)) {
          Assert.assertEquals("RUNNING", status.getStatus());
        } else {
          Assert.assertEquals("STOPPED", status.getStatus());
        }
      }

      LOG.info("Stopping flow");
      programClient.stop(flow);
      assertProgramStopped(programClient, flow);

      testWorkflowCommand(Id.Program.from(app, ProgramType.WORKFLOW, FakeWorkflow.NAME));

      LOG.info("Starting flow with debug");
      programClient.start(flow, true);
      assertProgramRunning(programClient, flow);
      programClient.stop(flow);
      assertProgramStopped(programClient, flow);
    } finally {
      try {
        appClient.delete(app);
      } catch (Exception e) {
        LOG.error("Error deleting app {} during test cleanup.", app, e);
      }
    }
  }

  private void testWorkflowCommand(final Id.Program workflow) throws Exception {
    // File is used to synchronized between the test case and the FakeWorkflow
    File doneFile = TMP_FOLDER.newFile();
    Assert.assertTrue(doneFile.delete());

    LOG.info("Starting workflow");

    programClient.start(workflow, false, ImmutableMap.of("done.file", doneFile.getAbsolutePath()));
    assertProgramRunning(programClient, workflow);
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getProgramRuns(workflow, "running", Long.MIN_VALUE, Long.MAX_VALUE, 100).size();
      }
    }, 5, TimeUnit.SECONDS);
    List<RunRecord> runRecords = programClient.getProgramRuns(workflow, "running", Long.MIN_VALUE, Long.MAX_VALUE, 100);
    Assert.assertEquals(1, runRecords.size());

    final String pid = runRecords.get(0).getPid();
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        try {
          return programClient.getWorkflowCurrent(workflow.getApplication(), workflow.getId(), pid).size();
        } catch (NotFoundException e) {
          // try again if the 'current' endpoint is not discoverable yet
          return 0;
        }
      }
    }, 20, TimeUnit.SECONDS);

    // Signal the FakeWorkflow that execution can be continued by creating temp file
    Assert.assertTrue(doneFile.createNewFile());

    assertProgramStopped(programClient, workflow);
    LOG.info("Workflow stopped");
  }
}
