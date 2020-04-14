/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A service that periodically relay messages from local TMS to the runtime server.
 * This service runs in the remote runtime.
 */
public class RuntimeClientService extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeClientService.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG,
                                                            LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));
  private static final Gson GSON = new Gson();

  private final Map<String, TopicRelayer> topicRelayers;
  private final MessagingContext messagingContext;
  private final long pollTimeMillis;
  private final long gracefulShutdownMillis;
  private final ProgramRunId programRunId;
  private final RuntimeClient runtimeClient;
  private final int fetchLimit;
  private long programFinishTime;

  @Inject
  RuntimeClientService(CConfiguration cConf, MessagingService messagingService,
                       DiscoveryServiceClient discoveryServiceClient, ProgramRunId programRunId) {
    super(RetryStrategies.fromConfiguration(cConf, "system.runtime.monitor."));
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.gracefulShutdownMillis = cConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS);
    this.programRunId = programRunId;
    this.runtimeClient = new RuntimeClient(discoveryServiceClient);
    this.fetchLimit = cConf.getInt(Constants.RuntimeMonitor.BATCH_SIZE);
    this.programFinishTime = -1L;
    this.topicRelayers = RuntimeMonitors.createTopicConfigs(cConf).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> createTopicRelayer(cConf, e.getValue())));
  }

  @Override
  protected long runTask() throws Exception {
    long nextPollDelay = pollTimeMillis;
    for (Map.Entry<String, TopicRelayer> entry : topicRelayers.entrySet()) {
      TopicRelayer topicRelayer = entry.getValue();
      nextPollDelay = Math.min(nextPollDelay, topicRelayer.publishMessages());
    }

    // If we got the program finished state, determine when to shutdown
    if (programFinishTime > 0) {
      // Gives half the time of the graceful shutdown time to allow empty fetches
      // Essentially is the wait time for any unpublished events on the remote runtime to publish
      // E.g. Metrics from the remote runtime process might have some delay after the program state changed,
      // even though we explicitly flush the metrics on program completion.
      // If the nextPollDelay returned by all topicRelays equals to the pollTimeMillis,
      // that means all of them fetched till the end of the corresponding topic in the latest fetch.
      long now = System.currentTimeMillis();
      if ((nextPollDelay == pollTimeMillis && now - (gracefulShutdownMillis >> 1) > programFinishTime)
          || (now - gracefulShutdownMillis > programFinishTime)) {
        LOG.debug("Program {} terminated. Shutting down runtime client service.", programRunId);
        stop();
      }
    }

    return nextPollDelay;
  }

  @Override
  protected boolean shouldRetry(Exception e) {
    OUTAGE_LOG.warn("Failed to send runtime status. Will be retried.", e);
    return true;
  }

  @Override
  protected void doShutdown() throws Exception {
    // Close all the TopicRelay, which will flush out all pending messages
    for (TopicRelayer topicRelayer : topicRelayers.values()) {
      Retries.callWithRetries((Retries.Callable<Void, IOException>) () -> {
        topicRelayer.close();
        return null;
      }, getRetryStrategy(), t -> t instanceof IOException || t instanceof RetryableException);
    }
  }

  @VisibleForTesting
  long getProgramFinishTime() {
    return programFinishTime;
  }

  /**
   * Creates an instance of {@link TopicRelayer} based on the topic.
   */
  private TopicRelayer createTopicRelayer(CConfiguration cConf, String topic) {
    TopicId topicId = NamespaceId.SYSTEM.topic(topic);

    String programStatusTopic = cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    if (programStatusTopic.equals(topic)) {
      return new ProgramStatusTopicRelayer(topicId);
    }
    return new TopicRelayer(topicId);
  }

  /**
   * Helper class to fetch and publish messages from one topic.
   */
  private class TopicRelayer implements Closeable {

    private final Logger progressLog = Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));

    private final TopicId topicId;
    private String lastMessageId;
    private long nextPublishTimeMillis;
    private int totalPublished;


    TopicRelayer(TopicId topicId) {
      this.topicId = topicId;
    }

    /**
     * Fetches messages from the {@link MessagingContext} and publish them using {@link RuntimeClient}.
     *
     * @return delay in milliseconds till the next poll
     * @throws TopicNotFoundException if the TMS topic to fetch from does not exist
     * @throws IOException if failed to read from TMS or write to RuntimeClient
     */
    long publishMessages() throws TopicNotFoundException, IOException, BadRequestException {
      long currentTimeMillis = System.currentTimeMillis();

      // Not too publish more than necessary in one topic.
      // This method might get called more than once even before the next publish time is hit.
      if (currentTimeMillis < nextPublishTimeMillis) {
        return nextPublishTimeMillis - currentTimeMillis;
      }

      try (CloseableIterator<Message> iterator = messagingContext.getMessageFetcher().fetch(topicId.getNamespace(),
                                                                                            topicId.getTopic(),
                                                                                            fetchLimit,
                                                                                            lastMessageId)) {
        AtomicInteger messageCount = new AtomicInteger();
        if (iterator.hasNext()) {
          String[] messageId = new String[1];
          processMessages(new AbstractIterator<Message>() {
            @Override
            protected Message computeNext() {
              if (!iterator.hasNext()) {
                return endOfData();
              }
              Message message = iterator.next();
              messageId[0] = message.getId();
              messageCount.incrementAndGet();
              return message;
            }
          });

          // Update the lastMessageId if sendMessages succeeded
          lastMessageId = messageId[0] == null ? lastMessageId : messageId[0];
          totalPublished += messageCount.get();
          progressLog.debug("Processed in total {} messages on topic {}", totalPublished, topicId);
        }

        // If we fetched all messages, then delay the next poll by pollTimeMillis.
        // Otherwise, try to poll again immediately.
        nextPublishTimeMillis = System.currentTimeMillis();
        if (messageCount.get() >= fetchLimit) {
          return 0L;
        }
        nextPublishTimeMillis += pollTimeMillis;
        return pollTimeMillis;
      }
    }

    /**
     * Processes the give list of {@link Message}. By default it sends them through the {@link RuntimeClient}.
     */
    protected void processMessages(Iterator<Message> iterator) throws IOException, BadRequestException {
      runtimeClient.sendMessages(programRunId, topicId, iterator);
    }

    @Override
    public void close() throws IOException {
      try {
        // Force one extra poll
        nextPublishTimeMillis = 0L;
        publishMessages();
      } catch (TopicNotFoundException | BadRequestException e) {
        // This shouldn't happen. If it does, it must be some bug in the system and there is no way to recover from it.
        // So just log the cause for debugging.
        LOG.error("Failed to publish messages on close for topic {}", topicId, e);
      }
    }
  }

  /**
   * A {@link TopicRelayer} specifically for handling program state events.
   * We need special handling for program state to delay the relaying of terminal program status
   * to give a grace period for messages in other topics to send out.
   */
  private class ProgramStatusTopicRelayer extends TopicRelayer {

    private final List<Message> lastProgramStateMessages;

    ProgramStatusTopicRelayer(TopicId topicId) {
      super(topicId);
      this.lastProgramStateMessages = new LinkedList<>();
    }

    @Override
    protected void processMessages(Iterator<Message> iterator) throws IOException, BadRequestException {
      List<Message> message = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
        .collect(Collectors.toList());

      if (programFinishTime < 0) {
        programFinishTime = findProgramFinishTime(message);
      }
      if (programFinishTime >= 0) {
        // Buffer the program state messages and don't publish them until the end
        // Otherwise, once we publish, the deprovisioner will kick in and delete the cluster
        // which could result in losing the last set of messages for some topics.
        // Since we already consumed the incoming Iterator, the next fetch offset is being updated.
        // This is to avoid fetching duplicate messages.
        lastProgramStateMessages.addAll(message);

        // Send an empty iterator to serve as the heartbeat.
        super.processMessages(Collections.emptyIterator());
      } else {
        // If the program is not yet finished, just publish the messages
        super.processMessages(message.iterator());
      }
    }

    @Override
    public void close() throws IOException {
      // Keep polling until it sees the program completion
      RetryStrategy retryStrategy = RetryStrategies.timeLimit(gracefulShutdownMillis, TimeUnit.MILLISECONDS,
                                                              getRetryStrategy());
      Retries.runWithRetries(() -> {
        ProgramStatusTopicRelayer.super.close();
        if (programFinishTime < 0) {
          throw new RetryableException("Program completion is not yet observed");
        }
      }, retryStrategy, t -> t instanceof IOException || t instanceof RetryableException);

      if (!lastProgramStateMessages.isEmpty()) {
        try {
          super.processMessages(lastProgramStateMessages.iterator());
        } catch (BadRequestException e) {
          // This shouldn't happen. If it does, that means the server thinks this program is no longer running.
          // The best we can do is to log here, even the log won't be collected by CDAP, but it will be retained
          // on the cluster.
          LOG.warn("Failed to send program state messages to runtime server: {}", lastProgramStateMessages, e);
        }
        lastProgramStateMessages.clear();
      }
    }

    /**
     * Returns the time where the program finished, meaning it reaches one of the terminal states. If the given
     * list of {@link Message} doesn't contain such information, {@code -1L} is returned.
     */
    private long findProgramFinishTime(List<Message> messages) {
      for (Message message : messages) {
        Notification notification = GSON.fromJson(message.getPayloadAsString(), Notification.class);
        if (notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
          continue;
        }

        Map<String, String> properties = notification.getProperties();
        String programRun = properties.get(ProgramOptionConstants.PROGRAM_RUN_ID);
        String programStatus = properties.get(ProgramOptionConstants.PROGRAM_STATUS);

        if (programRun == null || programStatus == null) {
          continue;
        }

        // Only match the program state change for the program run it is monitoring
        // For Workflow case, there could be multiple state changes for programs running inside the workflow.
        ProgramRunId messageRunId = GSON.fromJson(programRun, ProgramRunId.class);
        if (!programRunId.equals(messageRunId)) {
          continue;
        }

        if (ProgramRunStatus.isEndState(programStatus)) {
          try {
            return Long.parseLong(properties.get(ProgramOptionConstants.END_TIME));
          } catch (Exception e) {
            // END_TIME should be a valid long. In case there is any problem, use the timestamp in the message ID
            return new MessageId(Bytes.fromHexString(message.getId())).getPublishTimestamp();
          }
        }
      }

      return -1L;
    }
  }
}
