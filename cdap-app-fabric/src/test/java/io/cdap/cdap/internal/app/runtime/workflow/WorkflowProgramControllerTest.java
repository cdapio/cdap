/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link WorkflowProgramController} state forwarding and stop/suspend/resume controls.
 */
public class WorkflowProgramControllerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testRunningAndCompletedStateForwarding() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowSpecification wfSpec = WorkflowDriverTest.createWorkflowSpec(Collections.emptyList());
    Program program = WorkflowDriverTest.createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());
    WorkflowDriver driver = WorkflowDriverTest.createDriver(
        cConf, program, wfSpec, programType -> null, null);
    ProgramRunId runId = program.getId().run(RunIds.generate());

    WorkflowProgramController controller = new WorkflowProgramController(runId, driver);
    CountDownLatch aliveLatch = new CountDownLatch(1);
    CountDownLatch completedLatch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void alive() {
        aliveLatch.countDown();
      }

      @Override
      public void completed() {
        completedLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    driver.start();
    Assert.assertTrue(aliveLatch.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(completedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(ProgramController.State.COMPLETED, controller.getState());
    program.close();
  }

  @Test
  public void testSuspendResumeAndStopTransitionsToKilled() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowActionNode actionNode = new WorkflowActionNode(
        "mrAction", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr1"));
    WorkflowSpecification wfSpec = WorkflowDriverTest.createWorkflowSpec(
        Collections.singletonList(actionNode));
    Program program = WorkflowDriverTest.createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());
    WorkflowDriver driver = WorkflowDriverTest.createDriver(
        cConf, program, wfSpec, programType -> null, null);
    ProgramRunId runId = program.getId().run(RunIds.generate());

    // Suspend before first node runs so workflow blocks until stopped
    driver.suspend();
    WorkflowProgramController controller = new WorkflowProgramController(runId, driver);
    CountDownLatch aliveLatch = new CountDownLatch(1);
    CountDownLatch killedLatch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void alive() {
        aliveLatch.countDown();
      }

      @Override
      public void killed() {
        killedLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    driver.start();
    Assert.assertTrue(aliveLatch.await(5, TimeUnit.SECONDS));

    controller.suspend().get(5, TimeUnit.SECONDS);
    Assert.assertEquals(ProgramController.State.SUSPENDED, controller.getState());

    // Stop while suspended interrupts and awaits driver termination
    controller.stop().get(5, TimeUnit.SECONDS);
    Assert.assertTrue(killedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(ProgramController.State.KILLED, controller.getState());
    program.close();
  }

  @Test
  public void testFailedServiceForwardsErrorToController() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowActionNode actionNode = new WorkflowActionNode(
        "mrAction", new ScheduleProgramInfo(SchedulableProgramType.MAPREDUCE, "mr1"));
    WorkflowSpecification wfSpec = WorkflowDriverTest.createWorkflowSpec(
        Collections.singletonList(actionNode));
    Program program = WorkflowDriverTest.createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());
    IllegalStateException failure = new IllegalStateException("workflow failed");
    WorkflowDriver driver = WorkflowDriverTest.createDriver(
        cConf, program, wfSpec, programType -> {
          throw failure;
        }, null);
    ProgramRunId runId = program.getId().run(RunIds.generate());

    WorkflowProgramController controller = new WorkflowProgramController(runId, driver);
    CountDownLatch errorLatch = new CountDownLatch(1);
    AtomicReference<Throwable> recordedFailure = new AtomicReference<>();
    controller.addListener(new AbstractListener() {
      @Override
      public void error(Throwable cause) {
        recordedFailure.set(cause);
        errorLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    driver.start();
    Assert.assertTrue(errorLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(ProgramController.State.ERROR, controller.getState());
    Assert.assertNotNull(recordedFailure.get());
    program.close();
  }
}
