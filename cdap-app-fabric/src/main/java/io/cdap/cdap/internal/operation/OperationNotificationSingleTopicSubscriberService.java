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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.services.AbstractNotificationSubscriberService;
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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that receives program status notifications from a single topic and persists to the store.
 * No transactions should be started in any of the overrided methods since they are already wrapped
 * in a transaction.
 */
class OperationNotificationSingleTopicSubscriberService
    extends AbstractNotificationSubscriberService {

  private static final Logger LOG =
      LoggerFactory.getLogger(OperationNotificationSingleTopicSubscriberService.class);

  private final OperationStatePublisher statePublisher;

  private final OperationLifecycleManager lifecycleService;

  OperationNotificationSingleTopicSubscriberService(
      MessagingService messagingService,
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      OperationStatePublisher statePublisher,
      TransactionRunner transactionRunner,
      String name,
      String topicName,
      OperationLifecycleManager lifecycleManager) {
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
    this.statePublisher = statePublisher;
    this.lifecycleService = lifecycleManager;
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
    while (messages.hasNext()) {
      ImmutablePair<String, Notification> messagePair = messages.next();
      processNotification(
          messagePair.getFirst().getBytes(StandardCharsets.UTF_8),
          messagePair.getSecond(),
          structuredTableContext);
    }
  }

  /**
   * Process a {@link Notification} received from TMS.
   *
   * @param messageIdBytes the raw message id in the TMS for the notification
   * @param notification the {@link Notification} to process
   * @param context context to get the table for operations
   * @throws Exception if failed to process the given notification
   */
  @VisibleForTesting
  void processNotification(
      byte[] messageIdBytes,
      Notification notification,
      StructuredTableContext context)
      throws Exception {
    OperationRunStore runStore = new OperationRunStore(context);
    try {
      OperationNotification operationNotification = OperationNotification.fromNotification(
          notification);
      handleOperationEvent(operationNotification, messageIdBytes, runStore);
    } catch (IllegalArgumentException | JsonSyntaxException e) {
      LOG.warn("Got invalid operation notification", e);
    }
  }

  private void handleOperationEvent(
      OperationNotification notification,
      byte[] messageIdBytes,
      OperationRunStore runStore
  ) throws Exception {
    LOG.debug("Processing operation status notification: {}", notification);
    OperationRunId runId = notification.getRunId();
    OperationRunDetail runDetail;
    try {
      runDetail = runStore.getOperation(runId);
      if (!runDetail.getRun().getStatus().canTransitionTo(notification.getStatus())) {
        LOG.debug(
            "Ignoring unexpected request to transition operation {} from {} state to "
                + "{} state.",
            runId, runDetail.getRun().getStatus(), notification.getStatus());
        return;
      }

      byte[] existingSourceId = runDetail.getSourceId();
      if (existingSourceId != null && Bytes.compareTo(messageIdBytes, existingSourceId) < 0) {
        LOG.debug(
            "Notification source id '{}' is not larger than the existing source id '{}' in the existing "
                + "operation run detail.",
            Bytes.toHexString(runDetail.getSourceId()), Bytes.toHexString(existingSourceId));
        return;
      }
    } catch (OperationRunNotFoundException e) {
      LOG.debug(String.format("Ignoring message for non existent operation %s", runId));
      return;
    }

    switch (notification.getStatus()) {
      case STARTING:
        String oldUser = SecurityRequestContext.getUserId();
        try {
          if (runDetail.getPrincipal() != null) {
            SecurityRequestContext.setUserId(runDetail.getPrincipal());
          }
          lifecycleService.startOperation(runDetail);
        } catch (Exception e) {
          LOG.error("Failed to start operation {}", runDetail, e);
          statePublisher.publishFailed(runId,
              new OperationError(e.getMessage(), Collections.emptyList()));
        } finally {
          SecurityRequestContext.setUserId(oldUser);
        }
        runStore.updateOperationStatus(runId, OperationRunStatus.STARTING, messageIdBytes);
        break;
      case RUNNING:
      case SUCCEEDED:
        runStore.updateOperationStatus(runId, notification.getStatus(), messageIdBytes);
        if (notification.getResources() != null) {
          runStore.updateOperationResources(runId, notification.getResources(), messageIdBytes);
        }
        break;
      case STOPPING:
        try {
          lifecycleService.stopOperation(runDetail);
        } catch (IllegalStateException e) {
          LOG.warn("Failed to stop operation {}", runDetail.getRunId(), e);
        }
        runStore.updateOperationStatus(runId, OperationRunStatus.STOPPING, messageIdBytes);
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
