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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ProxyDrainState}.
 */
public class ProxyDrainStateTest {

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void testDrainingStartsOff() {
    ProxyDrainState drainState = new ProxyDrainState();
    Assert.assertFalse(drainState.isDraining());
    Assert.assertTrue(drainState.tryStartRequest());
    Assert.assertEquals(1, drainState.getInFlight());
    drainState.requestFinished();

    drainState.startDraining();

    Assert.assertTrue(drainState.isDraining());
    Assert.assertFalse(drainState.tryStartRequest());
    Assert.assertEquals(0, drainState.getInFlight());
  }

  @Test
  public void testAwaitDrainedReturnsAtOnceWhenNothingIsInFlight() throws Exception {
    ProxyDrainState drainState = new ProxyDrainState();
    drainState.requestStarted();
    drainState.requestFinished();

    Assert.assertTrue(drainState.awaitDrained(0));
  }

  @Test
  public void testAwaitDrainedReturnsWhenTheLastRequestFinishes() throws Exception {
    ProxyDrainState drainState = new ProxyDrainState();
    drainState.requestStarted();
    drainState.requestStarted();
    executor.schedule(drainState::requestFinished, 100, TimeUnit.MILLISECONDS);
    executor.schedule(drainState::requestFinished, 300, TimeUnit.MILLISECONDS);

    long start = System.currentTimeMillis();
    Assert.assertTrue(drainState.awaitDrained(TimeUnit.SECONDS.toMillis(30)));

    // Woken by the last finish, not by the 5s progress-log interval or the timeout.
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue("Took " + elapsed + "ms", elapsed < 3000);
    Assert.assertEquals(0, drainState.getInFlight());
  }

  @Test
  public void testAwaitDrainedGivesUpAtTheTimeout() throws Exception {
    ProxyDrainState drainState = new ProxyDrainState();
    drainState.requestStarted();

    long start = System.currentTimeMillis();
    Assert.assertFalse(drainState.awaitDrained(200));

    Assert.assertTrue(System.currentTimeMillis() - start >= 180);
    Assert.assertEquals(1, drainState.getInFlight());
  }
}
