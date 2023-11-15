/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.twill.internal.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that creates children services, each to handle a single partition of operation status
 * events topic.
 */
public class OperationNotificationSubscriberService extends AbstractIdleService {
  private static final Logger LOG =
      LoggerFactory.getLogger(OperationNotificationSubscriberService.class);

  private final MessagingService messagingService;
  private final CConfiguration cConf;
  private final OperationStatePublisher statePublisher;
  private final TransactionRunner transactionRunner;
  private final MetricsCollectionService metricsCollectionService;
  private final OperationLifecycleManager lifecycleManager;
  private Service delegate;

  @Inject
  OperationNotificationSubscriberService(
      MessagingService messagingService,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      OperationStatePublisher statePublisher,
      TransactionRunner transactionRunner,
      OperationLifecycleManager lifecycleManager) {

    this.messagingService = messagingService;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.statePublisher = statePublisher;
    this.transactionRunner = transactionRunner;
    this.lifecycleManager = lifecycleManager;
  }

  @Override
  protected void startUp() throws Exception {
    // first process the existing active runs before starting to listen
    RetryStrategy retryStrategy =
        RetryStrategies.fromConfiguration(cConf, Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX);

    TransactionRunners.run(
        transactionRunner, context -> {
          Retries.runWithRetries(
              () -> {
                processStartingOperations(context);
                processRunningOperations(context);
                processStoppingOperations(context);
              },
              retryStrategy,
              e -> true
          );
        }
    );

    List<Service> children = new ArrayList<>();
    String topicPrefix = cConf.get(Constants.Operation.STATUS_EVENT_TOPIC);
    int numPartitions = cConf.getInt(Constants.Operation.STATUS_EVENT_NUM_PARTITIONS);
    IntStream.range(0, numPartitions)
        .forEach(i -> children.add(createChildService("operation.status." + i, topicPrefix + i)));
    delegate = new CompositeService(children);

    delegate.startAndWait();
  }

  // Sends STARTING notification for all STARTING operations
  private void processStartingOperations(StructuredTableContext context) throws Exception {
    new OperationRunStore(context).scanOperationByStatus(
        OperationRunStatus.STARTING,
        (runDetail) -> {
          LOG.debug("Retrying to start operation {}.", runDetail.getRunId());
          statePublisher.publishStarting(runDetail.getRunId());
        }
    );
  }

  // Try to resume run for all RUNNING operations
  private void processRunningOperations(StructuredTableContext context) throws Exception {
    new OperationRunStore(context).scanOperationByStatus(
        OperationRunStatus.RUNNING,
        (runDetail) -> {
          lifecycleManager.isRunning(runDetail, statePublisher);
        }
    );
  }

  // Sends STOPPING notification for all STOPPING operations
  private void processStoppingOperations(StructuredTableContext context) throws Exception {
    new OperationRunStore(context).scanOperationByStatus(
        OperationRunStatus.STOPPING,
        (runDetail) -> {
          LOG.debug("Retrying to stop operation {}.", runDetail.getRunId());
          statePublisher.publishStopping(runDetail.getRunId());
        }
    );
  }

  @Override
  protected void shutDown() throws Exception {
    delegate.stopAndWait();
  }

  private OperationNotificationSingleTopicSubscriberService createChildService(
      String name, String topicName) {
    return new OperationNotificationSingleTopicSubscriberService(
        messagingService,
        cConf,
        metricsCollectionService,
        statePublisher,
        transactionRunner,
        name,
        topicName,
        lifecycleManager
    );
  }
}


