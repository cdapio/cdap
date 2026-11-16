/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link DelayedProgramController}
 */
public class DelayedProgramControllerTest {

  @Test
  public void testListener() throws InterruptedException {
    ProgramRunId programRunId = new ProgramRunId("ns", "app", ProgramType.SPARK, "program", RunIds.generate().getId());
    DelayedProgramController controller = new DelayedProgramController(programRunId);

    CountDownLatch aliveLatch = new CountDownLatch(1);
    CountDownLatch killedLatch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (currentState == ProgramController.State.ALIVE) {
          alive();
        } else if (currentState == ProgramController.State.KILLED) {
          completed();
        }
      }

      @Override
      public void alive() {
        aliveLatch.countDown();
      }

      @Override
      public void killed() {
        killedLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Perform state change on the delegate
    ProgramController delegate = new AbstractProgramController(programRunId) {

      {
        started();
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
      protected void doCommand(String name, Object value) {
        // no-op
      }
    };
    controller.setProgramController(delegate);
    Assert.assertTrue(aliveLatch.await(5, TimeUnit.SECONDS));

    controller.stop();
    Assert.assertTrue(killedLatch.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testCallDelegateFutureCallbackSuccessAndFailure() throws Exception {
    ProgramRunId programRunId =
        new ProgramRunId("ns", "app", ProgramType.SPARK, "program", RunIds.generate().getId());
    DelayedProgramController controller = new DelayedProgramController(programRunId);
    Assert.assertEquals(ProgramController.State.STARTING, controller.getState());

    AtomicReference<String> receivedCommand = new AtomicReference<>();
    ListenableFuture<ProgramController> pendingCommand = controller.command("scale", 4);

    ProgramController delegate = new AbstractProgramController(programRunId) {
      {
        started();
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
      protected void doCommand(String name, Object value) {
        if ("fail".equals(name)) {
          throw new IllegalArgumentException("command failed");
        }
        receivedCommand.set(name + "=" + value);
      }
    };

    controller.setProgramController(delegate);
    Assert.assertSame(delegate, pendingCommand.get(5, TimeUnit.SECONDS));
    Assert.assertEquals("scale=4", receivedCommand.get());

    Assert.assertSame(delegate, controller.suspend().get(5, TimeUnit.SECONDS));
    Assert.assertSame(delegate, controller.resume().get(5, TimeUnit.SECONDS));

    // Verify FutureCallback.onFailure propagates the exact failure cause to resultFuture.
    ListenableFuture<ProgramController> failingCommand = controller.command("fail", 0);
    ExecutionException thrown = Assert.assertThrows(
        ExecutionException.class, () -> failingCommand.get(5, TimeUnit.SECONDS));
    Assert.assertTrue(thrown.getCause() instanceof IllegalArgumentException);
    Assert.assertEquals("command failed", thrown.getCause().getMessage());
  }
}
