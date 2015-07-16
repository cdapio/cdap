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
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
      appClient.delete(app);
    }
  }

  private void testWorkflowCommand(final Id.Program workflow) throws Exception {
    // File is used to synchronized between the test case and the FakeWorkflow
    File doneFile = TMP_FOLDER.newFile();
    Assert.assertTrue(doneFile.delete());

    LOG.info("Starting workflow");

    programClient.start(workflow, false, ImmutableMap.of("done.file", doneFile.getAbsolutePath()));
    assertProgramRunning(programClient, workflow);
    List<RunRecord> runRecords = programClient.getProgramRuns(workflow, "running", Long.MIN_VALUE, Long.MAX_VALUE, 100);
    Assert.assertEquals(1, runRecords.size());

    final String pid = runRecords.get(0).getPid();
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getWorkflowCurrent(workflow.getApplication(), workflow.getId(), pid).size();
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Signal the FakeWorkflow that execution can be continued by creating temp file
    Assert.assertTrue(doneFile.createNewFile());

    assertProgramStopped(programClient, workflow);
    LOG.info("Workflow stopped");
  }
}
