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

package io.cdap.cdap.scheduler;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ScheduleNotificationSubscriberService}.
 */
public class ScheduleNotificationSubscriberServiceTest {

  @Test
  public void testStartUpWaitsForSlowChildRunningAndShutDownHandlesChildFailure() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Scheduler.EVENT_POLL_DELAY_MILLIS, 50L);
    MessagingService messagingService = Mockito.mock(MessagingService.class);
    TransactionRunner transactionRunner = Mockito.mock(TransactionRunner.class);

    ScheduleNotificationSubscriberService service = new ScheduleNotificationSubscriberService(
        cConf, messagingService, new NoOpMetricsCollectionService(), transactionRunner);

    Service slowChild = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        Thread.sleep(120L);
      }

      @Override
      protected void shutDown() throws Exception {
        Thread.sleep(20L);
      }
    };

    Service failingStopChild = new AbstractIdleService() {
      @Override
      protected void startUp() {
        // Immediate start.
      }

      @Override
      protected void shutDown() {
        throw new IllegalStateException("simulated child stop failure");
      }
    };

    Field subscribersField =
        ScheduleNotificationSubscriberService.class.getDeclaredField("subscriberServices");
    subscribersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Service> children = (List<Service>) subscribersField.get(service);
    Assert.assertEquals(3, children.size());
    children.set(0, slowChild);
    children.set(1, failingStopChild);
    Service defaultChild = children.get(2);

    service.startAsync().awaitRunning();
    try {
      // Immediately upon startAndWait() returning, all child subscriber services MUST be RUNNING.
      Assert.assertEquals(Service.State.RUNNING, slowChild.state());
      Assert.assertEquals(Service.State.RUNNING, failingStopChild.state());
      Assert.assertEquals(Service.State.RUNNING, defaultChild.state());
    } finally {
      service.stopAsync().awaitTerminated();
    }

    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(Service.State.TERMINATED, slowChild.state());
    Assert.assertEquals(Service.State.FAILED, failingStopChild.state());
    Assert.assertEquals(Service.State.TERMINATED, defaultChild.state());
  }
}
