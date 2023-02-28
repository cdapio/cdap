/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link ProgramController}.
 */
public class ProgramControllerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramControllerTest.class);

  @Test
  public void testInitState() throws ExecutionException, InterruptedException {
    // To test against race-condition, we start/stop service multiple times.
    // If there is race, there is a chance that this test will fail in some env.
    // Otherwise it should always pass
    ExecutorService executor = Executors.newCachedThreadPool();

    int serviceCount = 1000;
    final CountDownLatch latch = new CountDownLatch(serviceCount);
    ProgramId programId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "test").service("test");
    for (int i = 0; i < serviceCount; i++) {
      // Creates a controller for a guava service do nothing in start/stop.
      // The short time in start creates a chance to have out-of-order init() and alive() call if there is a race.
      Service service = new TestService(0, 0);
      ProgramController controller = new ProgramControllerServiceAdapter(service, programId.run(RunIds.generate()));
      ListenableFuture<Service.State> startCompletion = service.start();

      controller.addListener(new AbstractListener() {
        private volatile boolean initCalled;

        @Override
        public void init(ProgramController.State currentState, @Nullable Throwable cause) {
          initCalled = true;
          if (currentState == ProgramController.State.ALIVE) {
            latch.countDown();
          }
        }

        @Override
        public void alive() {
          if (initCalled) {
            latch.countDown();
          } else {
            LOG.error("init() not called before alive()");
          }
        }
      }, executor);

      startCompletion.get();
      service.stopAndWait();
    }

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testAddListenerRace() throws InterruptedException, ExecutionException, TimeoutException {
    ProgramRunId runId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());

    // Testing race condition described in https://cdap.atlassian.net/browse/CDAP-19827
    // Basically is the race condition between addListener and the executor thread when there is thread
    // interruption to the caller thread of the addListener method.
    for (int i = 0; i < 100; i++) {
      TestController controller = new TestController(runId);
      CompletableFuture<ProgramController.State> future = new CompletableFuture<>();
      Thread t = new Thread(() -> {
        try {
          controller.addListener(new AbstractListener() {
            @Override
            public void init(ProgramController.State currentState, @Nullable Throwable cause) {
              future.complete(currentState);
            }
          }, Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          // Interrupted exception is expected
          if (!(e.getCause() instanceof InterruptedException)) {
            future.completeExceptionally(e.getCause());
          }
        }
      });
      t.start();
      t.interrupt();
      t.join();

      Assert.assertEquals(ProgramController.State.STARTING, future.get(5, TimeUnit.SECONDS));
    }
  }

  private static final class TestService extends AbstractIdleService {

    private final long startDelay;
    private final long stopDelay;

    private TestService(long startDelay, long stopDelay) {
      this.startDelay = startDelay;
      this.stopDelay = stopDelay;
    }

    @Override
    protected void startUp() throws Exception {
      TimeUnit.MILLISECONDS.sleep(startDelay);
    }

    @Override
    protected void shutDown() throws Exception {
      TimeUnit.MILLISECONDS.sleep(stopDelay);
    }
  }

  private static final class TestController extends AbstractProgramController {

    TestController(ProgramRunId programRunId) {
      super(programRunId);
    }

    @Override
    protected void doSuspend() throws Exception {

    }

    @Override
    protected void doResume() throws Exception {

    }

    @Override
    protected void doStop() throws Exception {

    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {

    }
  }
}
