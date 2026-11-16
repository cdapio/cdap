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

package io.cdap.cdap.internal.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationResourceScopedError;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link InMemoryOperationController}.
 */
public class InMemoryOperationControllerTest {

  @Test
  public void testCompleteSuccessAndResources() throws Exception {
    OperationRunId runId = new OperationRunId("ns1", "run1");
    OperationStatePublisher publisher = Mockito.mock(OperationStatePublisher.class);
    Set<OperationResource> resources = ImmutableSet.of(new OperationResource("app1"));
    LongRunningOperation operation = context -> Futures.immediateFuture(resources);
    LongRunningOperationContext context = new LongRunningOperationContext(runId, publisher);
    OperationDriver driver = new OperationDriver(operation, context);
    InMemoryOperationController controller =
        new InMemoryOperationController(runId, publisher, driver);

    driver.start();
    OperationController completed = controller.complete().get(5, TimeUnit.SECONDS);

    Assert.assertSame(controller, completed);
    Mockito.verify(publisher).publishRunning(runId);
    Mockito.verify(publisher).publishResources(runId, resources);
    Mockito.verify(publisher).publishSuccess(runId);
    Mockito.verify(publisher, Mockito.never()).publishKilled(Mockito.any());
  }

  @Test
  public void testStopPublishesKilled() throws Exception {
    OperationRunId runId = new OperationRunId("ns1", "run-stop");
    OperationStatePublisher publisher = Mockito.mock(OperationStatePublisher.class);
    CountDownLatch startedLatch = new CountDownLatch(1);
    CountDownLatch stopLatch = new CountDownLatch(1);
    LongRunningOperation operation = context -> {
      startedLatch.countDown();
      try {
        stopLatch.await();
      } catch (InterruptedException e) {
        // Interrupted by OperationDriver.triggerShutdown() while transitioning to STOPPING.
      }
      return Futures.immediateFuture(Collections.emptySet());
    };
    LongRunningOperationContext context = new LongRunningOperationContext(runId, publisher);
    OperationDriver driver = new OperationDriver(operation, context);
    InMemoryOperationController controller =
        new InMemoryOperationController(runId, publisher, driver);

    driver.start();
    Assert.assertTrue(startedLatch.await(5, TimeUnit.SECONDS));

    OperationController stopped = controller.stop().get(5, TimeUnit.SECONDS);
    Assert.assertSame(controller, stopped);
    Assert.assertTrue(controller.complete().isDone());
    Mockito.verify(publisher).publishRunning(runId);
    Mockito.verify(publisher).publishKilled(runId);
    Mockito.verify(publisher, Mockito.never()).publishSuccess(Mockito.any());
  }

  @Test
  public void testOperationExceptionAndGenericFailure() throws Exception {
    OperationRunId runId1 = new OperationRunId("ns1", "run-op-ex");
    OperationStatePublisher publisher1 = Mockito.mock(OperationStatePublisher.class);
    OperationResourceScopedError scopedError =
        new OperationResourceScopedError("uri1", "bad resource");
    LongRunningOperation failingOp = context -> {
      throw new OperationException("operation failed", ImmutableList.of(scopedError));
    };
    OperationDriver driver1 =
        new OperationDriver(failingOp, new LongRunningOperationContext(runId1, publisher1));
    InMemoryOperationController controller1 =
        new InMemoryOperationController(runId1, publisher1, driver1);

    driver1.start();
    controller1.complete().get(5, TimeUnit.SECONDS);

    ArgumentCaptor<OperationError> errorCaptor1 = ArgumentCaptor.forClass(OperationError.class);
    Mockito.verify(publisher1).publishFailed(Mockito.eq(runId1), errorCaptor1.capture());
    Assert.assertEquals("operation failed", errorCaptor1.getValue().getMessage());
    Assert.assertEquals(ImmutableList.of(scopedError), errorCaptor1.getValue().getDetails());

    OperationRunId runId2 = new OperationRunId("ns1", "run-rt-ex");
    OperationStatePublisher publisher2 = Mockito.mock(OperationStatePublisher.class);
    LongRunningOperation runtimeFailingOp = context -> {
      throw new IllegalStateException("unexpected crash");
    };
    OperationDriver driver2 =
        new OperationDriver(runtimeFailingOp, new LongRunningOperationContext(runId2, publisher2));
    InMemoryOperationController controller2 =
        new InMemoryOperationController(runId2, publisher2, driver2);

    driver2.start();
    controller2.complete().get(5, TimeUnit.SECONDS);

    ArgumentCaptor<OperationError> errorCaptor2 = ArgumentCaptor.forClass(OperationError.class);
    Mockito.verify(publisher2).publishFailed(Mockito.eq(runId2), errorCaptor2.capture());
    Assert.assertEquals("unexpected crash", errorCaptor2.getValue().getMessage());
    Assert.assertEquals(Collections.emptyList(), errorCaptor2.getValue().getDetails());
  }
}
