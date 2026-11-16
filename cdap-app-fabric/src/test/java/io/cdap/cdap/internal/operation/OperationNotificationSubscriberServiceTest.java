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

package io.cdap.cdap.internal.operation;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link OperationNotificationSubscriberService}.
 */
public class OperationNotificationSubscriberServiceTest {

  @Test
  public void testStartUpAndShutDownDelegatesToCompositeService() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Operation.STATUS_EVENT_NUM_PARTITIONS, 2);
    cConf.setLong(Constants.Operation.STATUS_EVENT_POLL_DELAY_MILLIS, 50L);

    MessagingService messagingService = Mockito.mock(MessagingService.class);
    OperationStatePublisher statePublisher = Mockito.mock(OperationStatePublisher.class);
    TransactionRunner transactionRunner = Mockito.mock(TransactionRunner.class);
    AtomicInteger txCalls = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      txCalls.incrementAndGet();
      return null;
    }).when(transactionRunner).run(Mockito.any(TxRunnable.class));
    OperationLifecycleManager lifecycleManager = Mockito.mock(OperationLifecycleManager.class);

    OperationNotificationSubscriberService service =
        new OperationNotificationSubscriberService(
            messagingService,
            cConf,
            new NoOpMetricsCollectionService(),
            statePublisher,
            transactionRunner,
            lifecycleManager);

    service.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Assert.assertTrue(txCalls.get() >= 1);

    Field delegateField =
        OperationNotificationSubscriberService.class.getDeclaredField("delegate");
    delegateField.setAccessible(true);
    Service delegate = (Service) delegateField.get(service);
    Assert.assertNotNull(delegate);
    Assert.assertEquals(Service.State.RUNNING, delegate.state());

    service.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(Service.State.TERMINATED, delegate.state());
  }
}
