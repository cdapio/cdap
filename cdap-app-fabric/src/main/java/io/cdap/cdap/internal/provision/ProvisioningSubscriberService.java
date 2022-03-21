/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.RunRecordMonitorService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

/**
 * Service that receives program status notifications and persists to the store.
 * No transactions should be started in any of the overrided methods since they are already wrapped in a transaction.
 */
public class ProvisioningSubscriberService extends ProvisionBaseSubscriberService {
  @Inject
  ProvisioningSubscriberService(MessagingService messagingService, CConfiguration cConf,
                                MetricsCollectionService metricsCollectionService,
                                ProvisioningService provisioningService,
                                ProgramStateWriter programStateWriter, TransactionRunner transactionRunner,
                                RunRecordMonitorService runRecordMonitorService) {
    super("provision", cConf.get(Constants.AppFabric.PROVISION_EVENT_TOPIC),
          cConf.getInt(Constants.AppFabric.PROVISION_EVENT_FETCH_SIZE),
          cConf.getLong(Constants.AppFabric.PROVISION_EVENT_POLL_DELAY_MILLIS),
          messagingService, cConf, metricsCollectionService, provisioningService, programStateWriter, transactionRunner,
          runRecordMonitorService);
  }
}
