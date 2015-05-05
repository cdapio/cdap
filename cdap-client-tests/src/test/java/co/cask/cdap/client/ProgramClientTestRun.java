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

import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.app.FakeWorkflow;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

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
    appClient.deploy(createAppJarFile(FakeApp.class));

    try {
      // start, scale, and stop flow
      verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(FakeApp.NAME, ProgramType.FLOW));

      LOG.info("Starting flow");
      programClient.start(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      assertProgramRunning(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

      LOG.info("Getting flow history");
      programClient.getAllProgramRuns(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME, 0, Long.MAX_VALUE,
                                   Integer.MAX_VALUE);

      LOG.info("Scaling flowlet");
      Assert.assertEquals(1, programClient.getFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME));
      programClient.setFlowletInstances(FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);
      assertFlowletInstances(programClient, FakeApp.NAME, FakeFlow.NAME, FakeFlow.FLOWLET_NAME, 3);

      LOG.info("Stopping flow");
      programClient.stop(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      assertProgramStopped(programClient, FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);

      testWorkflowCommand();

    } finally {
      appClient.delete(FakeApp.NAME);
    }
  }

  private void testWorkflowCommand() throws Exception {
    // File is used to synchronized between the test case and the FakeWorkflow
    File doneFile = new File("/tmp/fakeworkflow.done");
    if (doneFile.exists()) {
      doneFile.delete();
    }

    LOG.info("Starting workflow");

    programClient.start(FakeApp.NAME, ProgramType.WORKFLOW, FakeWorkflow.NAME);
    assertProgramRunning(programClient, FakeApp.NAME, ProgramType.WORKFLOW, FakeWorkflow.NAME);
    List<RunRecord> runRecords = programClient.getProgramRuns(FakeApp.NAME, ProgramType.WORKFLOW, FakeWorkflow.NAME,
                                                              "running", Long.MIN_VALUE, Long.MAX_VALUE, 100);
    Assert.assertEquals(1, runRecords.size());
    List<WorkflowActionNode> nodes = programClient.getWorkflowCurrent(FakeApp.NAME, FakeWorkflow.NAME,
                                                                      runRecords.get(0).getPid());
    Assert.assertEquals(1, nodes.size());

    // Signal the FakeWorkflow that execution can be continued by creating temp file
    doneFile.createNewFile();

    assertProgramStopped(programClient, FakeApp.NAME, ProgramType.WORKFLOW, FakeWorkflow.NAME);
    LOG.info("Workflow stopped");

    doneFile.delete();
  }
}
