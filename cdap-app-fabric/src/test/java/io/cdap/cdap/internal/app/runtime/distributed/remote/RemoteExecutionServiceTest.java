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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteExecutionService}.
 */
public class RemoteExecutionServiceTest {

  private ScheduledExecutorService scheduler;

  /**
   * Initializes a single-threaded daemon scheduler for each test.
   */
  @Before
  public void setUp() {
    scheduler = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("remote-exec-test-%d"));
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
  public void testGetServiceNameAndSelfStopAfterSecondNotRunningCheck() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 1L);

    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkProg").run(RunIds.generate());
    RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
    Mockito.when(processController.isRunning()).thenReturn(false);
    ProgramStateWriter stateWriter = Mockito.mock(ProgramStateWriter.class);

    RemoteExecutionService service = new RemoteExecutionService(
        cConf, runId, scheduler, processController, stateWriter);

    Assert.assertEquals("runtime-service-" + runId.getRun(), service.serviceName());

    service.startAsync().awaitRunning();
    Tasks.waitFor(
        Service.State.TERMINATED, service::state, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    Mockito.verify(processController, Mockito.atLeast(2)).isRunning();
    Mockito.verify(stateWriter)
        .error(Mockito.eq(runId), Mockito.any(IllegalStateException.class));
  }

  @Test
  public void testShouldRetryAndHandleRetriesExhausted() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    ProgramRunId runId = NamespaceId.DEFAULT.app("app").spark("sparkProg").run(RunIds.generate());
    RemoteProcessController processController = Mockito.mock(RemoteProcessController.class);
    ProgramStateWriter stateWriter = Mockito.mock(ProgramStateWriter.class);

    RemoteExecutionService service = new RemoteExecutionService(
        cConf, runId, scheduler, processController, stateWriter);

    Mockito.when(processController.isRunning()).thenReturn(true);
    Assert.assertTrue(service.shouldRetry(new IOException("transient ssh failure")));

    Mockito.when(processController.isRunning()).thenReturn(false);
    Assert.assertFalse(service.shouldRetry(new IOException("process gone")));
    Mockito.verify(stateWriter)
        .error(Mockito.eq(runId), Mockito.any(IllegalStateException.class));

    IOException exhaustedFailure = new IOException("retries exhausted");
    try {
      service.handleRetriesExhausted(exhaustedFailure);
      Assert.fail("Expected IOException to be rethrown");
    } catch (IOException thrown) {
      Assert.assertSame(exhaustedFailure, thrown);
    }
    Mockito.verify(processController).kill(RuntimeJobStatus.RUNNING);
  }
}
