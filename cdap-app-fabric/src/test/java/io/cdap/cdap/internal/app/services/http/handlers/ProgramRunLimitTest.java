/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.AppWithServicesAndWorker;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.test.SlowTests;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for {@link ProgramLifecycleHttpHandler} */
public class ProgramRunLimitTest extends AppFabricTestBase {
  private static final String STOPPED = "STOPPED";
  private static final String RUNNING = "RUNNING";
  private static final String STARTING = "STARTING";

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = createBasicCconf();
    // we enable Kerberos for these unit tests, so we can test namespace group permissions (see
    // testDataDirCreation).
    cConf.setInt(Constants.AppFabric.MAX_CONCURRENT_RUNS, 2);
    cConf.setInt(Constants.AppFabric.MAX_CONCURRENT_LAUNCHING, 1);
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, 1);
    initializeAndStartServices(cConf);
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentServiceLaunchingAndRunningLimit() throws Exception {
    // Launching/running a new service should NOT be controlled by flow-control mechanism.
    // deploy, check the status
    deploy(
        AppWithServicesAndWorker.class,
        200,
        Constants.Gateway.API_VERSION_3_TOKEN,
        TEST_NAMESPACE1);

    ApplicationId appId = new ApplicationId(TEST_NAMESPACE1, AppWithServicesAndWorker.NAME);
    ProgramId noOpService = appId.service(AppWithServicesAndWorker.NO_OP_SERVICE);
    ProgramId pingService = appId.service(AppWithServicesAndWorker.PING_SERVICE);
    ProgramId noOpWorker = appId.worker(AppWithServicesAndWorker.NO_OP_WORKER);

    // all programs are stopped initially
    Assert.assertEquals(STOPPED, getProgramStatus(noOpService));
    Assert.assertEquals(STOPPED, getProgramStatus(pingService));
    Assert.assertEquals(STOPPED, getProgramStatus(noOpWorker));

    // start both services and check the status
    startProgram(noOpService);
    waitState(noOpService, RUNNING);

    startProgram(pingService);
    waitState(pingService, RUNNING);

    // starting the worker should succeed
    startProgram(noOpWorker);
    Tasks.waitFor(
        1,
        () -> getProgramRuns(noOpWorker, ProgramRunStatus.COMPLETED).size(),
        10,
        TimeUnit.SECONDS);

    // stop one service
    stopProgram(noOpService);
    waitState(noOpService, STOPPED);

    // stop other service
    stopProgram(pingService);
    waitState(pingService, STOPPED);
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentWorkflowLaunchingAndRunningLimit() throws Exception {
    // Deploy, check the status
    deploy(AppWithWorkflow.class, 200);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, AppWithWorkflow.NAME);
    final Id.Workflow workflowId = Id.Workflow.from(appId, AppWithWorkflow.SampleWorkflow.NAME);

    // Ensures program state is stopped
    Assert.assertEquals(STOPPED, getProgramStatus(workflowId));

    // Start the program and wait until it goes into STARTING state
    startProgram(
        workflowId,
        ImmutableMap.of(
            "inputPath",
            createInput("input"),
            "outputPath",
            new File(tmpFolder.newFolder(), "output").getAbsolutePath()));
    waitState(workflowId, STARTING);

    // Starting the second run should fail since only 1 launching run is allowed
    startProgram(workflowId, 429);

    // Continue waiting for the first run to start running
    waitState(workflowId, RUNNING);
    Tasks.waitFor(
        2, () -> getProgramRuns(workflowId, ProgramRunStatus.ALL).size(), 10, TimeUnit.SECONDS);

    // There should be one REJECTED run and one RUNNING runs
    Assert.assertEquals(
        1,
        getProgramRuns(workflowId, ProgramRunStatus.ALL).stream()
            .filter(runRecord -> runRecord.getStatus() == ProgramRunStatus.RUNNING)
            .count());
    Assert.assertEquals(
        1,
        getProgramRuns(workflowId, ProgramRunStatus.ALL).stream()
            .filter(runRecord -> runRecord.getStatus() == ProgramRunStatus.REJECTED)
            .count());

    // Start the second run to have two active runs
    startProgram(
        workflowId,
        ImmutableMap.of(
            "inputPath",
            createInput("input2"),
            "outputPath",
            new File(tmpFolder.newFolder(), "output2").getAbsolutePath()));
    Tasks.waitFor(
        3, () -> getProgramRuns(workflowId, ProgramRunStatus.ALL).size(), 10, TimeUnit.SECONDS);

    // starting the third run should fail since there are two active runs already
    // I.e., one running and one in launching/running
    startProgram(workflowId, 429);

    // Wait until all runs finish
    waitState(workflowId, STOPPED);

    // There should be two REJECTED runs and two COMPLETED runs
    Assert.assertEquals(
        2,
        getProgramRuns(workflowId, ProgramRunStatus.ALL).stream()
            .filter(runRecord -> runRecord.getStatus() == ProgramRunStatus.COMPLETED)
            .count());
    Assert.assertEquals(
        2,
        getProgramRuns(workflowId, ProgramRunStatus.ALL).stream()
            .filter(runRecord -> runRecord.getStatus() == ProgramRunStatus.REJECTED)
            .count());
  }

  private String createInput(String folderName) throws IOException {
    File inputDir = tmpFolder.newFolder(folderName);

    File inputFile = new File(inputDir.getPath() + "/words.txt");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile.toPath(), Charsets.UTF_8)) {
      writer.write("this text has");
      writer.newLine();
      writer.write("two words text inside");
    }
    return inputDir.getAbsolutePath();
  }
}
