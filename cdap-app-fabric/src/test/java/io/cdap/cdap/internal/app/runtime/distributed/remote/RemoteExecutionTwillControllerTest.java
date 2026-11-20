/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.ServiceController;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteExecutionTwillController}.
 */
public class RemoteExecutionTwillControllerTest {

  private ScheduledExecutorService scheduler;

  /**
   * Initializes a single-threaded daemon scheduler for each test.
   */
  @Before
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("remote-ctrl-test-%d"));
  }

  /**
   * Shuts down the test scheduler.
   */
  @After
  public void tearDown() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void testServiceTerminatedCompletesControllerAndCompleteStopsService() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);
    cConf.setLong(Constants.RuntimeMonitor.REMOTE_STOP_DELAY_SECS, 1L);

    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
    RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
    Mockito.when(processController.isRunning()).thenReturn(true);
    Mockito.when(processController.getStatus()).thenReturn(RuntimeJobStatus.COMPLETED);

    RemoteExecutionService service = new RemoteExecutionService(
        cConf, runId, scheduler, processController, Mockito.mock(ProgramStateWriter.class));
    CompletableFuture<Void> startupStage = CompletableFuture.completedFuture(null);
    RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
        cConf, runId, startupStage, processController, scheduler, service, false);

    CountDownLatch terminatedLatch = new CountDownLatch(1);
    controller.onTerminated(terminatedLatch::countDown, Threads.SAME_THREAD_EXECUTOR);

    service.startAndWait();
    Assert.assertNull(controller.getTerminationStatus());

    controller.complete();
    controller.awaitTerminated(5, TimeUnit.SECONDS);
    Assert.assertTrue(terminatedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(
        ServiceController.TerminationStatus.SUCCEEDED, controller.getTerminationStatus());
    Mockito.verify(processController).kill(RuntimeJobStatus.COMPLETED);
  }

  @Test
  public void testReleaseStopsServiceWithoutCompletingController() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);

    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
    RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
    Mockito.when(processController.isRunning()).thenReturn(true);

    RemoteExecutionService service = new RemoteExecutionService(
        cConf, runId, scheduler, processController, Mockito.mock(ProgramStateWriter.class));
    RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
        cConf, runId, CompletableFuture.completedFuture(null),
        processController, scheduler, service, false);

    service.startAndWait();
    controller.release();

    Tasks.waitFor(
        Service.State.TERMINATED, service::state, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertNull(controller.getTerminationStatus());
  }

  @Test
  public void testServiceFailureMarksControllerFailedViaListener() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 1L);

    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkJob").run(RunIds.generate());
    RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
    Mockito.when(processController.isRunning()).thenReturn(false);

    IOException fatalFailure = new IOException("monitor task crashed");
    RemoteExecutionService failingService = new RemoteExecutionService(
        cConf, runId, scheduler, processController, Mockito.mock(ProgramStateWriter.class)) {
      @Override
      void doRunTask() throws Exception {
        throw fatalFailure;
      }
    };

    RemoteExecutionTwillController controller = new RemoteExecutionTwillController(
        cConf, runId, CompletableFuture.completedFuture(null),
        processController, scheduler, failingService, false);

    failingService.start();
    try {
      controller.awaitTerminated(5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException when executionService fails");
    } catch (ExecutionException expected) {
      Assert.assertSame(fatalFailure, expected.getCause());
    }
    Assert.assertEquals(
        ServiceController.TerminationStatus.FAILED, controller.getTerminationStatus());
  }
}
