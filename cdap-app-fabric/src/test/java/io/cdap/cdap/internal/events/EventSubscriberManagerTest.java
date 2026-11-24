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

package io.cdap.cdap.internal.events;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.features.Feature;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link EventSubscriberManager}.
 */
public class EventSubscriberManagerTest {

  private static class FakeEventSubscriber extends EventSubscriber {
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final long startupDelayMillis;
    private final boolean failOnInitialize;

    private FakeEventSubscriber(long startupDelayMillis, boolean failOnInitialize) {
      this.startupDelayMillis = startupDelayMillis;
      this.failOnInitialize = failOnInitialize;
    }

    @Override
    public void initialize() {
      if (failOnInitialize) {
        throw new IllegalStateException("subscriber init failed");
      }
      initialized.set(true);
    }

    @Override
    protected void startUp() throws Exception {
      if (startupDelayMillis > 0) {
        TimeUnit.MILLISECONDS.sleep(startupDelayMillis);
      }
    }

    @Override
    protected void runOneIteration() {
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedDelaySchedule(10, 10, TimeUnit.SECONDS);
    }
  }

  @Test
  public void testDisabledFeatureSkipsSubscribers() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.EVENT_READER.getFeatureFlagString(), false);
    FakeEventSubscriber subscriber = new FakeEventSubscriber(0L, false);

    EventSubscriberManager manager =
        new EventSubscriberManager(cConf, Collections.singleton(subscriber));
    manager.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, manager.state());
    Assert.assertFalse(subscriber.initialized.get());
    Assert.assertEquals(Service.State.NEW, subscriber.state());

    manager.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, manager.state());
    Assert.assertEquals(Service.State.NEW, subscriber.state());
  }

  @Test
  public void testEnabledFeatureWaitsForSubscribersAndContinuesOnFailure() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean("feature." + Feature.EVENT_READER.getFeatureFlagString(), true);

    FakeEventSubscriber failingSubscriber = new FakeEventSubscriber(0L, true);
    FakeEventSubscriber slowSubscriber = new FakeEventSubscriber(80L, false);
    Set<EventSubscriber> subscribers =
        new LinkedHashSet<>(Arrays.asList(failingSubscriber, slowSubscriber));

    EventSubscriberManager manager = new EventSubscriberManager(cConf, subscribers);
    manager.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, manager.state());
    Assert.assertTrue(slowSubscriber.initialized.get());
    Assert.assertEquals(Service.State.RUNNING, slowSubscriber.state());
    Assert.assertEquals(Service.State.NEW, failingSubscriber.state());

    manager.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, manager.state());
    Assert.assertEquals(Service.State.TERMINATED, slowSubscriber.state());
  }
}
