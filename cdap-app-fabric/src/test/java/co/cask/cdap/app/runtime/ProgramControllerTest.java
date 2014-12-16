/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.runtime;

import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.internal.RunIds;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    for (int i = 0; i < serviceCount; i++) {
      // Creates a controller for a guava service do nothing in start/stop.
      // The short time in start creates a chance to have out-of-order init() and alive() call if there is a race.
      Service service = new TestService(0, 0);
      ProgramController controller = new ProgramControllerServiceAdapter(service, "Test", RunIds.generate());
      ListenableFuture<Service.State> startCompletion = service.start();

      controller.addListener(new AbstractListener() {
        private volatile boolean initCalled;

        @Override
        public void init(ProgramController.State currentState) {
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
}
