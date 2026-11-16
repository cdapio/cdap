/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.spi.MessagingService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.Scheduler;

/**
 * Unit tests for {@link DistributedTimeSchedulerService}.
 */
public class DistributedTimeSchedulerServiceTest {

  @Test
  public void testStartUpAndShutDownWithRetryOnServiceUnavailable() throws Exception {
    Scheduler quartzScheduler = Mockito.mock(Scheduler.class);
    AtomicInteger attempts = new AtomicInteger(0);

    TimeScheduler timeScheduler = new TimeScheduler(
        () -> {
          if (attempts.incrementAndGet() == 1) {
            throw new ServiceUnavailableException("dataset.service");
          }
          return quartzScheduler;
        },
        Mockito.mock(MessagingService.class),
        CConfiguration.create(),
        new NoOpMetricsCollectionService());

    DistributedTimeSchedulerService service = new DistributedTimeSchedulerService(timeScheduler);
    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    Assert.assertEquals(2, attempts.get());
    Assert.assertTrue(timeScheduler.isStarted());
    Mockito.verify(quartzScheduler).start();

    Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
    Mockito.verify(quartzScheduler).shutdown();
  }
}
