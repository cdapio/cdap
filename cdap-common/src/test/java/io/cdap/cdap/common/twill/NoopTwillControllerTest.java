/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.twill.api.Command;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link NoopTwillController}.
 */
public class NoopTwillControllerTest {

  @Test
  public void testNoopTwillControllerOperationsAndLifecycle() throws Exception {
    NoopTwillController controller = new NoopTwillController();
    Assert.assertNotNull(controller.getRunId());
    Assert.assertNull(controller.getResourceReport());
    controller.addLogHandler(logEntry -> { });

    CountDownLatch runningLatch = new CountDownLatch(1);
    CountDownLatch terminatedLatch = new CountDownLatch(1);
    controller.onRunning(runningLatch::countDown, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(terminatedLatch::countDown, Threads.SAME_THREAD_EXECUTOR);

    ServiceDiscovered discovered = controller.discoverService("my-service");
    Assert.assertEquals("my-service", discovered.getName());
    Assert.assertFalse(discovered.iterator().hasNext());
    Assert.assertFalse(discovered.contains(
        new Discoverable("my-service",
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 8080))));
    Cancellable cancellable = discovered.watchChanges(sd -> { }, Threads.SAME_THREAD_EXECUTOR);
    cancellable.cancel();

    Command cmd = Command.Builder.of("ping").build();
    Assert.assertSame(cmd, controller.sendCommand(cmd).get(5, TimeUnit.SECONDS));
    Assert.assertSame(cmd, controller.sendCommand("runnable", cmd).get(5, TimeUnit.SECONDS));

    assertUnsupported(controller.changeInstances("r", 2));
    assertUnsupported(controller.restartAllInstances("r"));
    assertUnsupported(controller.restartInstances(
        Collections.singletonMap("r", Collections.singleton(0))));
    assertUnsupported(controller.restartInstances("r", 0, 1));
    assertUnsupported(controller.restartInstances("r", Collections.singleton(0)));
    assertUnsupported(controller.updateLogLevels(
        Collections.singletonMap("ROOT", LogEntry.Level.DEBUG)));
    assertUnsupported(controller.updateLogLevels("r",
        Collections.singletonMap("ROOT", LogEntry.Level.INFO)));
    assertUnsupported(controller.resetLogLevels("ROOT"));
    assertUnsupported(controller.resetRunnableLogLevels("r", "ROOT"));

    // Start and kill controller lifecycle.
    controller.startAsync().awaitRunning();
    Assert.assertTrue(runningLatch.await(5, TimeUnit.SECONDS));
    controller.kill();
    Assert.assertTrue(terminatedLatch.await(5, TimeUnit.SECONDS));
  }

  private static void assertUnsupported(Future<?> future) throws InterruptedException {
    try {
      future.get();
      Assert.fail("Expected ExecutionException with UnsupportedOperationException");
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
    }
  }
}
