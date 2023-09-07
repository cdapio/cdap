/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.twill.internal.CompositeService;

/**
 * Service that creates children services, each to handle a single partition of program status
 * events topic
 */
public class ProgramNotificationSubscriberService extends AbstractIdleService {
  private final MessagingService messagingService;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final OperationLifeCycleService operationLifecycleService;
  private final ProvisionerNotifier provisionerNotifier;
  private final ProgramLifecycleService programLifecycleService;
  private final ProvisioningService provisioningService;
  private final ProgramStateWriter programStateWriter;
  private final TransactionRunner transactionRunner;
  private final Store store;
  private final RunRecordMonitorService runRecordMonitorService;
  private Service delegate;
  private Set<ProgramCompletionNotifier> programCompletionNotifiers;

  @Inject
  ProgramNotificationSubscriberService(
      MessagingService messagingService,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      OperationLifeCycleService operationLifecycleService,
      ProvisionerNotifier provisionerNotifier,
      ProgramLifecycleService programLifecycleService,
      ProvisioningService provisioningService,
      ProgramStateWriter programStateWriter,
      TransactionRunner transactionRunner,
      Store store,
      RunRecordMonitorService runRecordMonitorService) {

    this.messagingService = messagingService;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.provisionerNotifier = provisionerNotifier;
    this.operationLifecycleService = operationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.provisioningService = provisioningService;
    this.programStateWriter = programStateWriter;
    this.transactionRunner = transactionRunner;
    this.store = store;
    this.runRecordMonitorService = runRecordMonitorService;
    this.programCompletionNotifiers = Collections.emptySet();
  }

  @Override
  protected void startUp() throws Exception {
    List<Service> children = new ArrayList<>();
    String topicPrefix = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    int numPartitions = cConf.getInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS);
    // Add bare one - we always listen to it
    children.add(createChildService("program.status", topicPrefix));
    // If number of partitions is more than 1 - create partitioned services
    if (numPartitions > 1) {
      IntStream.range(0, numPartitions)
          .forEach(i -> children.add(createChildService("program.status." + i, topicPrefix + i)));
    }
    delegate = new CompositeService(children);

    delegate.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    delegate.stopAndWait();
  }

  @Inject(optional = true)
  void setProgramCompletionNotifiers(Set<ProgramCompletionNotifier> notifiers) {
    this.programCompletionNotifiers = notifiers;
  }

  private ProgramNotificationSingleTopicSubscriberService createChildService(
      String name, String topicName) {
    return new ProgramNotificationSingleTopicSubscriberService(
        messagingService,
        cConf,
        metricsCollectionService,
        operationLifecycleService,
        provisionerNotifier,
        programLifecycleService,
        provisioningService,
        programStateWriter,
        transactionRunner,
        store,
        runRecordMonitorService,
        name,
        topicName,
        programCompletionNotifiers);
  }
}

