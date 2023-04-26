/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.program;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes program state and heartbeat messages through the messaging service
 */
public class MessagingProgramStatePublisher implements ProgramStatePublisher {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingProgramStatePublisher.class);
  private static final Gson GSON =
      ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
          .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
          .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();
  private final MessagingService messagingService;
  private final List<TopicId> topicIds;
  private final RetryStrategy retryStrategy;

  /**
   * Create a publisher that writes to MessagingService topics depending on the message content.
   *
   * @param cConf configuration containing the topic prefix and number of partitions
   * @param messagingService messaging service to write messages to
   */
  @Inject
  public MessagingProgramStatePublisher(CConfiguration cConf, MessagingService messagingService) {
    this(
        messagingService,
        cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC),
        cConf.getInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS),
        RetryStrategies.fromConfiguration(
            cConf, Constants.AppFabric.PROGRAM_STATUS_RETRY_STRATEGY_PREFIX)
    );
  }

  /**
   * Create a publisher that writes to MessagingService topics depending on the message content.
   *
   * @param messagingService messaging service to write messages to
   * @param topicPrefix prefix of the topic(s) to write to. If there is one topic, the prefix
   *     will be the topic name. If there is more than one topic, the topic name will be the
   *     prefix followed by the topic number
   * @param numTopics number of topics to write to
   * @param retryStrategy retry strategy to use for failures
   */
  @VisibleForTesting
  public MessagingProgramStatePublisher(MessagingService messagingService,
      String topicPrefix, int numTopics, RetryStrategy retryStrategy) {
    this.messagingService = messagingService;
    this.topicIds =
        numTopics == 1 ? Collections.singletonList(NamespaceId.SYSTEM.topic(topicPrefix)) :
            Collections.unmodifiableList(IntStream
                .range(0, numTopics)
                .mapToObj(i -> NamespaceId.SYSTEM.topic(topicPrefix + i))
                .collect(Collectors.toList()));
    this.retryStrategy = retryStrategy;
  }

  private TopicId getTopic(Notification programStatusNotification) {
    if (topicIds.size() == 1) {
      return topicIds.get(0);
    }
    String programRunIdStr = programStatusNotification.getProperties()
        .get(ProgramOptionConstants.PROGRAM_RUN_ID);
    if (programRunIdStr == null) {
      return topicIds.get(0);
    }
    ProgramRunId programRunId = GSON.fromJson(programRunIdStr, ProgramRunId.class);
    ApplicationId applicationId = programRunId.getParent().getParent();
    return topicIds.get(Math.abs(applicationId.hashCode()) % topicIds.size());
  }

  /**
   * Publish a notification to a topic. The topic chosen is based on the application id for
   * the notification.
   *
   * @param notificationType type of the notification of the message
   * @param properties properties of the message to publish, assumed to contain the program run id
   */
  public void publish(Notification.Type notificationType, Map<String, String> properties) {
    // ProgramRunId is always required in a notification
    Notification programStatusNotification = new Notification(notificationType, properties);

    int failureCount = 0;
    long startTime = -1L;
    boolean done = false;
    // TODO CDAP-12255 this code was basically copied from MessagingMetricsCollectionService.TopicPayload#publish.
    // This should be refactored into a common class for publishing to TMS with a retry strategy
    while (!done) {
      try {
        messagingService.publish(StoreRequestBuilder.of(getTopic(programStatusNotification))
            .addPayload(GSON.toJson(programStatusNotification))
            .build());
        LOG.trace("Published program status notification: {}", programStatusNotification);
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
}
