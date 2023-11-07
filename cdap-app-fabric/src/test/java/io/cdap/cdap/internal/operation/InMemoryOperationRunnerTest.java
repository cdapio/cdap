/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;


import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import java.time.Instant;
import java.util.Collections;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class InMemoryOperationRunnerTest {

  private InMemoryOperationRunner runner;

  private static Injector injector;
  private static final OperationStatePublisher mockPublisher = Mockito.mock(
      OperationStatePublisher.class);
  private static final InMemorySourceControlOperationRunner mockScmRunner = Mockito.mock(
      InMemorySourceControlOperationRunner.class);
  private static final ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
  private static final OperationRunId runId = new OperationRunId("namespace", "run");
  private static final OperationRun run = OperationRun.builder()
      .setRunId(runId.getRun())
      .setStatus(OperationRunStatus.PENDING)
      .setType(OperationType.PULL_APPS)
      .setMetadata(
          new OperationMeta(Collections.emptySet(), Instant.now(), null))
      .build();
  private static final OperationRunDetail detail = OperationRunDetail.builder()
      .setSourceId(AppFabricTestHelper.createSourceId(0))
      .setRunId(runId)
      .setRun(run)
      .setPullAppsRequest(new PullAppsRequest(ImmutableSet.of("1", "2", "3", "4"), null))
      .build();

  @BeforeClass
  public static void setupClass() {
    CConfiguration cconf = CConfiguration.create();
    injector = AppFabricTestHelper.getInjector(cconf, new AbstractModule() {
      @Override
      protected void configure() {
        bind(OperationStatePublisher.class).toInstance(mockPublisher);
        bind(InMemorySourceControlOperationRunner.class).toInstance(mockScmRunner);
        bind(ApplicationManager.class).toInstance(mockManager);
      }
    });
  }

  @Before
  public void setupTest() {
    runner = injector.getInstance(InMemoryOperationRunner.class);
    Mockito.reset(mockPublisher);
  }

  @Test
  public void testRun() throws Exception {

    OperationController controller = runner.run(detail);
    controller.completionFuture().get();

    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning(runId);
    Mockito.verify(mockPublisher, Mockito.times(1)).publishSuccess(runId);
  }

  @Test
  public void testRunWithError() throws Exception {

    Mockito.doThrow(new RuntimeException("test")).when(mockScmRunner).pull(
        Mockito.any(), Mockito.any()
    );

    OperationController controller = runner.run(detail);
    controller.completionFuture().get();

    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning(runId);
    Mockito.verify(mockPublisher, Mockito.times(1)).publishFailed(Mockito.eq(runId), Mockito.any());
    Mockito.verify(mockPublisher, Mockito.never()).publishSuccess(runId);
  }
}

