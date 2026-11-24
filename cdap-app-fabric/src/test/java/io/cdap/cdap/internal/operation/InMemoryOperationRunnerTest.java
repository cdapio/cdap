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
import com.google.common.util.concurrent.Futures;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsOperation;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsOperationFactory;
import io.cdap.cdap.internal.app.sourcecontrol.PullAppsRequest;
import io.cdap.cdap.internal.app.sourcecontrol.PushAppsOperationFactory;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link InMemoryOperationRunner}.
 */
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
      .setStatus(OperationRunStatus.STARTING)
      .setType(OperationType.PULL_APPS)
      .setMetadata(
          OperationMeta.builder().setCreateTime(Instant.now()).build())
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

  @AfterClass
  public static void tearDownClass() {
    AppFabricTestHelper.shutdown();
  }

  @Before
  public void setupTest() {
    runner = injector.getInstance(InMemoryOperationRunner.class);
    Mockito.reset(mockPublisher);
  }

  @Test
  public void testRun() throws Exception {

    OperationController controller = runner.run(detail);
    controller.complete().get();

    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning(runId);
    Mockito.verify(mockPublisher, Mockito.times(1)).publishSuccess(runId);
  }

  @Test
  public void testRunWithError() throws Exception {

    Mockito.doThrow(new RuntimeException("test")).when(mockScmRunner).multiPull(
        Mockito.any(), Mockito.any()
    );

    OperationController controller = runner.run(detail);
    controller.complete().get();

    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning(runId);
    Mockito.verify(mockPublisher, Mockito.times(1)).publishFailed(Mockito.eq(runId), Mockito.any());
    Mockito.verify(mockPublisher, Mockito.never()).publishSuccess(runId);
  }

  @Test
  public void testDirectRunAndStop() throws Exception {
    OperationStatePublisher localPublisher = Mockito.mock(OperationStatePublisher.class);
    PullAppsOperationFactory pullFactory = Mockito.mock(PullAppsOperationFactory.class);
    PushAppsOperationFactory pushFactory = Mockito.mock(PushAppsOperationFactory.class);
    CountDownLatch startedLatch = new CountDownLatch(1);
    CountDownLatch stopLatch = new CountDownLatch(1);
    PullAppsOperation pullOp = Mockito.mock(PullAppsOperation.class);
    Mockito.when(pullOp.run(Mockito.any())).thenAnswer(invocation -> {
      startedLatch.countDown();
      try {
        stopLatch.await();
      } catch (InterruptedException e) {
        // Interrupted by OperationDriver.triggerShutdown() while transitioning to STOPPING.
      }
      return Futures.immediateFuture(Collections.emptySet());
    });
    Mockito.when(pullFactory.create(Mockito.any())).thenReturn(pullOp);

    InMemoryOperationRunner directRunner =
        new InMemoryOperationRunner(localPublisher, pullFactory, pushFactory);
    OperationController controller = directRunner.run(detail);
    Assert.assertTrue(startedLatch.await(5, TimeUnit.SECONDS));
    controller.stop().get(5, TimeUnit.SECONDS);

    Mockito.verify(localPublisher).publishRunning(runId);
    Mockito.verify(localPublisher).publishKilled(runId);
  }
}

