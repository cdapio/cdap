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

package io.cdap.cdap.common.internal.remote;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that {@link TaskWorkerManagerService} waits for in-flight task requests when it stops.
 */
public class TaskWorkerManagerServiceTest {

  private CConfiguration cConf;
  private InMemoryDiscoveryService discoveryService;
  private ProxyDrainState drainState;

  @Before
  public void setUp() {
    cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorkerManager.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.TaskWorkerManager.PORT, 0);
    discoveryService = new InMemoryDiscoveryService();
    drainState = new ProxyDrainState();
  }

  @Test
  public void testStopWaitsForInFlightRequests() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();
    drainState.requestStarted();

    ListenableFuture<Service.State> stopped = service.stop();

    // Deregistered and draining, but still running while the request is in flight.
    waitUntilDeregistered();
    Assert.assertTrue(drainState.isDraining());
    TimeUnit.MILLISECONDS.sleep(300);
    Assert.assertFalse(stopped.isDone());

    drainState.requestFinished();
    Assert.assertEquals(Service.State.TERMINATED, stopped.get(10, TimeUnit.SECONDS));
  }

  @Test
  public void testStopGivesUpAtTheDrainTimeout() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 1);
    TaskWorkerManagerService service = startService();
    drainState.requestStarted();

    long start = System.currentTimeMillis();
    Assert.assertEquals(Service.State.TERMINATED, service.stop().get(10, TimeUnit.SECONDS));

    Assert.assertTrue(System.currentTimeMillis() - start >= TimeUnit.SECONDS.toMillis(1));
    Assert.assertEquals(1, drainState.getInFlight());
  }

  @Test
  public void testStopIsImmediateWhenNothingIsInFlight() throws Exception {
    cConf.setLong(Constants.TaskWorkerManager.DRAIN_TIMEOUT_SECS, 30);
    TaskWorkerManagerService service = startService();

    long start = System.currentTimeMillis();
    Assert.assertEquals(Service.State.TERMINATED, service.stop().get(10, TimeUnit.SECONDS));

    Assert.assertTrue(System.currentTimeMillis() - start < TimeUnit.SECONDS.toMillis(10));
  }

  private TaskWorkerManagerService startService() {
    TaskWorkerManagerService service =
        new TaskWorkerManagerService(cConf, discoveryService, discoveryService, drainState);
    service.startAndWait();
    Assert.assertTrue(discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER)
        .iterator().hasNext());
    return service;
  }

  private void waitUntilDeregistered() throws InterruptedException {
    long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
    while (discoveryService.discover(Constants.Service.TASK_WORKER_MANAGER).iterator().hasNext()) {
      Assert.assertTrue("Still registered after 10s", System.currentTimeMillis() < deadline);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }
}
