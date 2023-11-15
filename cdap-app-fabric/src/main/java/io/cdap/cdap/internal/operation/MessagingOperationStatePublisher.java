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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Operation;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides capabilities to send operation lifecycle specific messages.
 */
public class MessagingOperationStatePublisher implements OperationStatePublisher {

  private final MessagingService messagingService;
  private final RetryStrategy retryStrategy;
  private final List<TopicId> topicIds;

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(
      MessagingOperationStatePublisher.class);

  /**
   * Create a publisher that writes to MessagingService topics depending on the message content.
   *
   * @param cConf configuration containing the topic prefix and number of partitions
   * @param messagingService messaging service to write messages to
   */
  @Inject
  public MessagingOperationStatePublisher(CConfiguration cConf, MessagingService messagingService) {
    this(
        messagingService,
        cConf.get(Operation.STATUS_EVENT_TOPIC),
        cConf.getInt(Operation.STATUS_EVENT_NUM_PARTITIONS),
        RetryStrategies.fromConfiguration(
            cConf, Operation.STATUS_RETRY_STRATEGY_PREFIX)
    );
  }

  /**
   * Create a publisher that writes to MessagingService topics depending on the message content.
   *
   * @param messagingService messaging service to write messages to
   * @param topicPrefix prefix of the topic(s) to write to. If there is one topic, the prefix
   *     will be the topic name. If there is more than one topic, the topic name will be the prefix
   *     followed by the topic number
   * @param numTopics number of topics to write to
   * @param retryStrategy retry strategy to use for failures
   */
  @VisibleForTesting
  public MessagingOperationStatePublisher(MessagingService messagingService,
      String topicPrefix, int numTopics, RetryStrategy retryStrategy) {
    this.messagingService = messagingService;
    this.topicIds = Collections.unmodifiableList(IntStream
        .range(0, numTopics)
        .mapToObj(i -> NamespaceId.SYSTEM.topic(topicPrefix + i))
        .collect(Collectors.toList()));
    this.retryStrategy = retryStrategy;
  }

  @Override
  public void publishResources(OperationRunId runId, Set<OperationResource> resources) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.RUNNING.name())
        .put(Operation.RESOURCES_NOTIFICATION_KEY, GSON.toJson(resources));

    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishRunning(OperationRunId runId) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.RUNNING.name());
    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishFailed(OperationRunId runId, OperationError error) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.FAILED.name())
        .put(Operation.ERROR_NOTIFICATION_KEY, GSON.toJson(error))
        .put(Operation.ENDTIME_NOTIFICATION_KEY, Instant.now().toString());
    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishSuccess(OperationRunId runId) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.SUCCEEDED.name())
        .put(Operation.ENDTIME_NOTIFICATION_KEY, Instant.now().toString());
    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishKilled(OperationRunId runId) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.KILLED.name())
        .put(Operation.ENDTIME_NOTIFICATION_KEY, Instant.now().toString());
    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishStopping(OperationRunId runId) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STOPPING.name());
    publish(runId, propertiesBuilder.build());
  }

  @Override
  public void publishStarting(OperationRunId runId) {
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
        .put(Operation.RUN_ID_NOTIFICATION_KEY, runId.toString())
        .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STARTING.name());
    publish(runId, propertiesBuilder.build());
  }

  /**
   * Publish a notification to a topic.
   *
   * @param runId {@link OperationRunId} for the notification
   * @param properties properties of the message to publish, assumed to contain the operation
   *     run id
   */
  public void publish(OperationRunId runId, Map<String, String> properties) {
    // OperationRunId is always required in a notification
    Notification notification = new Notification(Notification.Type.OPERATION_STATUS, properties);

    int failureCount = 0;
    long startTime = -1L;
    boolean done = false;
    // TODO CDAP-12255 This should be refactored into a common class for publishing to TMS with a retry strategy
    while (!done) {
      try {
        messagingService.publish(StoreRequestBuilder.of(getTopic(runId))
            .addPayload(GSON.toJson(notification))
            .build());
        LOG.trace("Published operation status notification: {}", notification);
        done = true;
      } catch (IOException | AccessException e) {
        throw Throwables.propagate(e);
      } catch (TopicNotFoundException | ServiceUnavailableException e) {
        // These exceptions are retry-able due to TMS not completely started
        if (startTime < 0) {
          startTime = System.currentTimeMillis();
        }
        long retryMillis = retryStrategy.nextRetry(++failureCount, startTime);
        if (retryMillis < 0) {
          LOG.error("Failed to publish messages to TMS and exceeded retry limit.", e);
          throw Throwables.propagate(e);
        }
        LOG.debug("Failed to publish messages to TMS due to {}. Will be retried in {} ms.",
            e.getMessage(), retryMillis);
        try {
          TimeUnit.MILLISECONDS.sleep(retryMillis);
        } catch (InterruptedException e1) {
          // Something explicitly stopping this thread. Simply just break and reset the interrupt flag.
          LOG.warn("Publishing message to TMS interrupted.");
          Thread.currentThread().interrupt();
          done = true;
        }
      }
    }
  }

  private TopicId getTopic(OperationRunId runId) {
    if (topicIds.size() == 1) {
      return topicIds.get(0);
    }
    return topicIds.get(Math.abs(runId.getRun().hashCode()) % topicIds.size());
  }
}
