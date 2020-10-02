/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.messaging.subscriber;

import com.google.common.collect.AbstractIterator;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Abstract base class for implementing message polling logic for reading messages from TMS.
 *
 * @param <T> the type that each message will be decoded to.
 */
public abstract class AbstractMessagingPollingService<T> extends AbstractRetryableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMessagingPollingService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));

  private final TopicId topicId;
  private final MetricsContext metricsContext;
  private final int fetchSize;
  private final long emptyFetchDelayMillis;
  private boolean messageIdInitialized;
  private String messageId;

  protected AbstractMessagingPollingService(TopicId topicId, MetricsContext metricsContext, int fetchSize,
                                            long emptyFetchDelayMillis, RetryStrategy retryStrategy) {
    super(retryStrategy);
    this.topicId = topicId;
    this.metricsContext = metricsContext;
    this.fetchSize = fetchSize;
    this.emptyFetchDelayMillis = emptyFetchDelayMillis;
  }

  /**
   * Returns the {@link TopicId} that this service is fetching from.
   */
  protected final TopicId getTopicId() {
    return topicId;
  }

  /**
   * Returns the {@link MessagingContext} that this service used for interacting with TMS.
   */
  protected abstract MessagingContext getMessagingContext();

  /**
   * Decodes the raw {@link Message} into an object of type {@code T}.
   *
   * @param message the {@link Message} to decode
   * @return an object of type {@code T}
   * @throws Exception if the decode failed and the given message will be skipped for processing
   */
  protected abstract T decodeMessage(Message message) throws Exception;

  /**
   * Loads last persisted message id.
   * The returned message id will be used as the starting message id (exclusive) for the first fetch.
   *
   * @return the last persisted message id or {@code null} to have first fetch starts from the first available message
   *         in the topic.
   * @throws IOException if failed to load the message id
   */
  @Nullable
  protected abstract String loadMessageId() throws IOException;

  /**
   * Processes the give list of messages. If {@link Exception} is raised from this method,
   * all messages as provided through the {@code messages} parameter will be replayed in the next call.
   *
   * @param messages an {@link Iterator} of {@link ImmutablePair}, with the {@link ImmutablePair#first}
   *                 as the message id, and the {@link ImmutablePair#second} as the decoded message
   * @return the message ID for the next fetch to start from (exclusively). If returning {@code null},
   *         the next fetch will start from the same message ID that was used to fetch the current batch of
   *         messages
   * @throws Exception if there is error processing messages.
   */
  @Nullable
  protected abstract String processMessages(Iterator<ImmutablePair<String, T>> messages) throws Exception;

  /**
   * Perform post processing after a batch of messages has been processed and before the next batch of
   * messages is fetched.
   */
  protected void postProcess() {
    // no-op
  }

  @Override
  protected boolean shouldRetry(Exception ex) {
    // Log the exception
    try {
      throw ex;
    } catch (ServiceUnavailableException e) {
      SAMPLING_LOG.warn("Failed to contact service {}. Will retry in next run.", e.getServiceName(), e);
    } catch (TopicNotFoundException e) {
      SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry in next run.", e);
    } catch (Exception e) {
      SAMPLING_LOG.warn("Failed to get and process notifications. Will retry in next run", e);
    }

    return true;
  }

  @Override
  protected final long runTask() throws Exception {
    long delayMillis = fetchAndProcessMessages();
    try {
      postProcess();
    } catch (Exception e) {
      LOG.warn("Failed to perform post processing after processing messages.", e);
    }
    return delayMillis;
  }

  /**
   * Fetching messages from TMS.
   */
  protected List<Message> fetchMessages(@Nullable String messageId) throws TopicNotFoundException, IOException {
    List<Message> messages = new ArrayList<>();
    LOG.trace("Fetching from topic '{}' with messageId '{}'", topicId, messageId);
    MessageFetcher messageFetcher = getMessagingContext().getMessageFetcher();
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(getTopicId().getNamespace(),
                                                                    getTopicId().getTopic(), fetchSize, messageId)) {
      while (iterator.hasNext() && state() == State.RUNNING) {
        messages.add(iterator.next());
      }
    }
    return messages;
  }

  /**
   * The method has the main logic to perform one fetch from TMS and process the fetched messages.
   *
   * @return number of milliseconds to sleep before the next fetch and process should happen.
   */
  private long fetchAndProcessMessages() throws Exception {
    // Fetch the messageId if hasn't been fetched
    if (!messageIdInitialized) {
      messageId = loadMessageId();
      messageIdInitialized = true;
    }

    long startTime = System.currentTimeMillis();

    // Collects batch of messages for processing.
    List<Message> messages = fetchMessages(messageId);
    metricsContext.gauge("tms.fetch.time.ms", System.currentTimeMillis() - startTime);
    metricsContext.increment("tms.fetch.messages", messages.size());

    // Return if stopping or request to sleep for configured number of milliseconds if there is no message fetched.
    if (messages.isEmpty() || state() != State.RUNNING) {
      return Math.max(0L, emptyFetchDelayMillis - (System.currentTimeMillis() - startTime));
    }

    startTime = System.currentTimeMillis();

    MessageIterator iterator = new MessageIterator(messages.iterator());
    String messageId = processMessages(iterator);
    this.messageId = messageId == null ? this.messageId : messageId;

    long endTime = System.currentTimeMillis();
    metricsContext.gauge("process.duration.ms", endTime - startTime);
    metricsContext.increment("process.notifications", iterator.getConsumedCount());

    // Calculate the delay
    if (messageId != null) {
      metricsContext.gauge("process.delay.ms", endTime - getMessagePublishTime(messageId));
    }

    // Poll again immediately
    return 0L;
  }

  /**
   * Returns the publish time encoded in the given message id.
   *
   * @param messageId the message id to decode
   * @return the publish time or {@code 0} if the message id is {@code null}.
   */
  private long getMessagePublishTime(String messageId) {
    return new MessageId(Bytes.fromHexString(messageId)).getPublishTimestamp();
  }

  /**
   * An {@link Iterator} that decodes {@link Message} to a given object type through the {@link #decodeMessage(Message)}
   * method.
   */
  private final class MessageIterator extends AbstractIterator<ImmutablePair<String, T>> {

    private final Iterator<Message> messages;
    private int consumedCount;

    MessageIterator(Iterator<Message> messages) {
      this.messages = messages;
      this.consumedCount = 0;
    }

    @Override
    protected ImmutablePair<String, T> computeNext() {
      // Decode the next message into Notification.
      while (messages.hasNext()) {
        Message message = messages.next();

        try {
          T decoded = decodeMessage(message);
          LOG.trace("Processing message from topic {} with message id {}: {}", getTopicId(), message.getId(), decoded);
          consumedCount++;
          return new ImmutablePair<>(message.getId(), decoded);
        } catch (Exception e) {
          // This shouldn't happen.
          LOG.warn("Failed to decode message with id {} and payload '{}'. Skipped.",
                   message.getId(), message.getPayloadAsString(), e);
        }

        consumedCount++;
      }
      return endOfData();
    }

    int getConsumedCount() {
      return consumedCount;
    }
  }
}
