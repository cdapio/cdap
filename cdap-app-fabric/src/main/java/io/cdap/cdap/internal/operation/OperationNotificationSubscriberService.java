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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Operation;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
import io.cdap.cdap.internal.app.services.RunRecordMonitorService;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.twill.internal.CompositeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that creates children services, each to handle a single partition of operation status
 * events topic
 */
public class OperationNotificationSubscriberService extends AbstractIdleService {

  private final MessagingService messagingService;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final OperationStatePublisher statePublisher;
  private final TransactionRunner transactionRunner;
  private final OperationLifecycleManager manager;
  private Service delegate;

  @Inject
  OperationNotificationSubscriberService(
      MessagingService messagingService,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      OperationStatePublisher statePublisher,
      TransactionRunner transactionRunner,
      RunRecordMonitorService runRecordMonitorService,
      OperationLifecycleManager manager) {

    this.messagingService = messagingService;
    this.cConf = cConf;
    this.metricsCollectionService = metricsCollectionService;
    this.statePublisher = statePublisher;
    this.transactionRunner = transactionRunner;
    this.manager = manager;
  }

  @Override
  protected void startUp() throws Exception {
    List<Service> children = new ArrayList<>();
    String topicPrefix = cConf.get(Constants.Operation.STATUS_EVENT_TOPIC);
    int numPartitions = cConf.getInt(Constants.Operation.STATUS_EVENT_NUM_PARTITIONS);
    // Add bare one - we always listen to it
    children.add(createChildService("operation.status", topicPrefix));
    // If number of partitions is more than 1 - create partitioned services
    if (numPartitions > 1) {
      IntStream.range(0, numPartitions)
          .forEach(i -> children.add(createChildService("operation.status." + i, topicPrefix + i)));
    }
    delegate = new CompositeService(children);

    delegate.startAndWait();
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
        manager);
  }
}


/**
 * Service that receives program status notifications from a single topic and persists to the store.
 * No transactions should be started in any of the overrided methods since they are already wrapped
 * in a transaction.
 */
class OperationNotificationSingleTopicSubscriberService
    extends AbstractNotificationSubscriberService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OperationNotificationSubscriberService.class);

  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() {
  }.getType();
  private static final Map<OperationRunStatus, String> STATUS_METRICS_NAME =
      ImmutableMap.of(
          OperationRunStatus.SUCCEEDED, Constants.Metrics.Operation.OPERATION_SUCCEEDED_RUNS,
          OperationRunStatus.KILLED, Constants.Metrics.Operation.OPERATION_FAILED_RUNS,
          OperationRunStatus.FAILED, Constants.Metrics.Operation.OPERATION_KILLED_RUNS
      );

  private final String recordedProgramStatusPublishTopic;
  private final OperationStatePublisher statePublisher;
  private final Queue<Runnable> tasks;
  private final MetricsCollectionService metricsCollectionService;
  private final CConfiguration cConf;

  private final OperationLifecycleManager manager;

  OperationNotificationSingleTopicSubscriberService(
      MessagingService messagingService,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      OperationStatePublisher statePublisher,
      TransactionRunner transactionRunner,
      String name,
      String topicName,
      OperationLifecycleManager manager) {
    super(
        name,
        cConf,
        topicName,
        cConf.getInt(Constants.Operation.STATUS_EVENT_FETCH_SIZE),
        cConf.getLong(Constants.Operation.STATUS_EVENT_POLL_DELAY_MILLIS),
        messagingService,
        metricsCollectionService,
        transactionRunner,
        cConf.getInt(Constants.Operation.STATUS_EVENT_TX_SIZE));
    this.recordedProgramStatusPublishTopic =
        cConf.get(Operation.STATUS_RECORD_EVENT_TOPIC);
    this.statePublisher = statePublisher;
    this.manager = manager;
    this.tasks = new LinkedList<>();
    this.metricsCollectionService = metricsCollectionService;
    this.cConf = cConf;
  }

  @Override
  protected void doStartUp() throws Exception {
    super.doStartUp();
    RetryStrategy retryStrategy =
        RetryStrategies.fromConfiguration(cConf, Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX);
    Retries.runWithRetries(
        this::startPendingRuns,
        retryStrategy,
        e -> true
    );
  }

  protected void startPendingRuns() throws OperationRunNotFoundException, IOException {
    int batchSize = cConf.getInt(Constants.Operation.INIT_BATCH_SIZE);
    manager.scanPendingOperations(
        batchSize,
        (runDetail) -> {
          LOG.debug("Retrying to start run {}.", runDetail.getRunId());
          statePublisher.publishStarting(runDetail.getRunId());
        }
    );
  }

  @Nullable
  @Override
  protected String loadMessageId(StructuredTableContext context)
      throws IOException, TableNotFoundException {
    return getAppMetadataStore(context).retrieveSubscriberState(getTopicId().getTopic(),
        "operation.state.reader");
  }

  @Override
  protected void storeMessageId(StructuredTableContext context, String messageId)
      throws IOException, TableNotFoundException {
    getAppMetadataStore(context).persistSubscriberState(getTopicId().getTopic(),
        "operation.state.reader", messageId);
  }

  @Override
  protected void processMessages(
      StructuredTableContext structuredTableContext,
      Iterator<ImmutablePair<String, Notification>> messages)
      throws Exception {
    List<Runnable> tasks = new LinkedList<>();
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      List<Runnable> runnables =
          processNotification(
              messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
              messagePair.getSecond(),
              structuredTableContext);
      tasks.addAll(runnables);
    }

    // Only add post-processing tasks if all messages are processed. If there is exception in the
    // processNotification,
    // messages will be replayed.
    this.tasks.addAll(tasks);
  }

  @Override
  protected void postProcess() {
    Runnable task = tasks.poll();
    while (task != null) {
      task.run();
      task = tasks.poll();
    }
  }

  /**
   * Process a {@link Notification} received from TMS.
   *
   * @param messageIdBytes the raw message id in the TMS for the notification
   * @param notification the {@link Notification} to process
   * @param context context to get the table for operations
   * @return a {@link List} of {@link Runnable} tasks to run after the transactional processing of
   *     the whole messages batch is completed
   * @throws Exception if failed to process the given notification
   */
  private List<Runnable> processNotification(
      byte[] messageIdBytes,
      Notification notification,
      StructuredTableContext context)
      throws Exception {
    OperationRunStore runStore = new OperationRunStore(context);

    OperationNotification operationNotification = OperationNotification.fromNotification(
        notification);

    if (notification.getNotificationType().equals(Notification.Type.OPERATION_HEART_BEAT)) {
      // TODO(samik) process heartbeat
      return Collections.emptyList();
    }

    List<Runnable> result = new ArrayList<>();
    handleOperationEvent(
        operationNotification,
        messageIdBytes,
        runStore,
        result
    );

    return result;
  }

  private void handleOperationEvent(
      OperationNotification notification,
      byte[] messageIdBytes,
      OperationRunStore runStore,
      List<Runnable> runnables
  ) throws Exception {
    LOG.trace("Processing program status notification: {}", notification);
    OperationRunId runId = notification.getRunId();
    switch (notification.getStatus()) {
      case STARTING:
        OperationRunDetail runDetail = runStore.getOperation(runId);
        if (runDetail != null
            && runDetail.getRun().getStatus() != OperationRunStatus.PENDING
            && runDetail.getRun().getStatus() != OperationRunStatus.STARTING) {
          // This is an invalid state transition happening. Valid state transitions are:
          // PENDING => STARTING : normal state transition
          // STARTING => STARTING : state transition after app-fabric restart
          LOG.debug(
              "Ignoring unexpected request to transition operation {} from {} state to "
                  + "STARTING state.",
              runId, runDetail.getRun().getStatus());
          return;
        }
        runnables.add(
            () -> {
              String oldUser = SecurityRequestContext.getUserId();
              try {
                SecurityRequestContext.setUserId(notification.getUser());
                try {
                  manager.startOperation(runId);
                } catch (Exception e) {
                  LOG.error("Failed to start operation {}", runDetail, e);
                  statePublisher.publishFailed(runId,
                      new OperationError(e.getMessage(), Collections.emptyList()));
                }
              } finally {
                SecurityRequestContext.setUserId(oldUser);
              }
            });

        runStore.updateOperationStatus(runId, OperationRunStatus.RUNNING, messageIdBytes);
        break;
      case RUNNING:
      case SUCCEEDED:
        runStore.updateOperationStatus(runId, notification.getStatus(), messageIdBytes);
        if (notification.getResources() != null) {
          runStore.updateOperationResources(runId, notification.getResources(), messageIdBytes);
        }
        break;
      case STOPPING:
        manager.stopOperation(runId);
        break;
      case KILLED:
        runStore.updateOperationStatus(runId, OperationRunStatus.KILLED, messageIdBytes);
        break;
      case FAILED:
        runStore.failOperationRun(runId, notification.getError(), notification.getEndTime(),
            messageIdBytes);
        break;
      default:
        // This should not happen
        LOG.error(
            "Unsupported status {} for operation {}, {}",
            notification.getStatus(),
            runId,
            notification);
    }
  }

  /**
   * Returns an instance of {@link AppMetadataStore}.
   */
  private AppMetadataStore getAppMetadataStore(StructuredTableContext context) {
    return AppMetadataStore.create(context);
  }
}


