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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractProgramController}.
 */
public class AbstractProgramControllerTest {

  private static ProgramRunId newRunId() {
    return NamespaceId.DEFAULT.app("app")
        .program(ProgramType.SERVICE, "svc")
        .run(RunIds.generate());
  }

  @Test
  public void testAddListenerDeduplicationCancellationAndLifecycle() throws Exception {
    TestController controller = new TestController(newRunId());

    AtomicReference<ProgramController.State> initState = new AtomicReference<>();
    AtomicInteger aliveCalls = new AtomicInteger();
    CountDownLatch suspendedLatch = new CountDownLatch(1);
    CountDownLatch killedLatch = new CountDownLatch(1);

    AbstractListener listener = new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        initState.set(currentState);
      }

      @Override
      public void alive() {
        aliveCalls.incrementAndGet();
      }

      @Override
      public void suspended() {
        suspendedLatch.countDown();
      }

      @Override
      public void killed() {
        killedLatch.countDown();
      }
    };

    Cancellable firstCancel = controller.addListener(listener, Threads.SAME_THREAD_EXECUTOR);
    Cancellable duplicateCancel = controller.addListener(listener, Threads.SAME_THREAD_EXECUTOR);
    Assert.assertSame(firstCancel, duplicateCancel);
    Assert.assertEquals(ProgramController.State.STARTING, initState.get());

    controller.triggerStarted();
    controller.suspend().get(5, TimeUnit.SECONDS);
    Assert.assertTrue(suspendedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(ProgramController.State.SUSPENDED, controller.getState());

    controller.resume().get(5, TimeUnit.SECONDS);
    Assert.assertEquals(ProgramController.State.ALIVE, controller.getState());
    Assert.assertEquals(2, aliveCalls.get());

    controller.stop(250L, TimeUnit.MILLISECONDS).get(5, TimeUnit.SECONDS);
    Assert.assertTrue(killedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(250L, controller.observedTimeoutMillis());

    firstCancel.cancel();
  }

  @Test
  public void testAddListenerWhenInterruptedWrapsInterruptedException() {
    TestController controller = new TestController(newRunId());
    // Block the controller's single-thread executor briefly so result.take() observes interrupt.
    CountDownLatch unblockExecutor = new CountDownLatch(1);
    controller.command("block", unblockExecutor);

    Thread.currentThread().interrupt();
    try {
      RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
          () -> controller.addListener(new AbstractListener() { }, Threads.SAME_THREAD_EXECUTOR));
      Assert.assertTrue(thrown.getCause() instanceof InterruptedException);
    } finally {
      // Clear interrupt status and release the controller executor.
      Thread.interrupted();
      unblockExecutor.countDown();
    }
  }

  private static final class TestController extends AbstractProgramController {

    private TestController(ProgramRunId programRunId) {
      super(programRunId);
    }

    private void triggerStarted() {
      started();
    }

    private long observedTimeoutMillis() {
      return getGracefulTimeoutMillis();
    }

    @Override
    protected void doSuspend() {
      // no-op
    }

    @Override
    protected void doResume() {
      // no-op
    }

    @Override
    protected void doStop() {
      // no-op
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      if ("block".equals(name) && value instanceof CountDownLatch) {
        ((CountDownLatch) value).await(5, TimeUnit.SECONDS);
      }
    }
  }
}
