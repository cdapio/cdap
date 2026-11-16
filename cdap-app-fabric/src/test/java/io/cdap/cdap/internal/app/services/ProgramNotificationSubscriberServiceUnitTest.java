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

package io.cdap.cdap.internal.app.services;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link ProgramNotificationSubscriberService} start/stop lifecycle.
 */
public class ProgramNotificationSubscriberServiceUnitTest {

  @Test
  public void testStartAndStopDelegateAndEmitFlowControlMetrics() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, 2);
    cConf.setLong(Constants.AppFabric.STATUS_EVENT_POLL_DELAY_MILLIS, 50L);

    MessagingService messagingService = Mockito.mock(MessagingService.class);
    Store store = Mockito.mock(Store.class);
    FlowControlService flowControlService = Mockito.mock(FlowControlService.class);
    TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);

    ProgramNotificationSubscriberService service = new ProgramNotificationSubscriberService(
        messagingService,
        cConf,
        new NoOpMetricsCollectionService(),
        Mockito.mock(ProvisionerNotifier.class),
        Mockito.mock(ProgramLifecycleService.class),
        Mockito.mock(ProgramRuntimeService.class),
        Mockito.mock(ProvisioningService.class),
        Mockito.mock(ProgramStateWriter.class),
        txRunner,
        store,
        flowControlService);

    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    Mockito.verify(store).scanActiveRuns(Mockito.anyInt(), Mockito.any());
    Mockito.verify(flowControlService).emitFlowControlMetrics();

    Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
  }
}
