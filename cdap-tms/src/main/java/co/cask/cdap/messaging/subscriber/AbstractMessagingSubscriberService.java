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

package co.cask.cdap.messaging.subscriber;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.messaging.data.MessageId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.xml.ws.handler.MessageContext;

/**
 * An abstract base class for implementing message consumption from TMS.
 * This service allows optional transactional fetch from TMS.
 * It always process messages and persisting consumer states in the same transaction.
 *
 * @param <T> the type that each message will be decoded to.
 */
public abstract class AbstractMessagingSubscriberService<T> extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMessagingSubscriberService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(10000));

  private final TopicId topicId;
  private final boolean transactionalFetch;
  private final int fetchSize;
  private final long emptyFetchDelayMillis;
  private final RetryStrategy retryStrategy;
  private final MetricsContext metricsContext;
  private boolean messageIdInitialized;
  private String messageId;
  private int failureCount;
  private long nonFailureStartTime;
  private ScheduledExecutorService executor;
  private long delay;

  /**
   * Constructor.
   *
   * @param topicId the topic to consume from
   * @param transactionalFetch {@code true} to indicate fetching from TMS needs to be performed inside transaction
   * @param fetchSize number of messages to fetch in each batch
   * @param emptyFetchDelayMillis number of milliseconds to sleep after a fetch returns empty result
   * @param retryStrategy the {@link RetryStrategy} to determine retry on failure
   * @param metricsContext the {@link MetricsContext} for emitting metrics about the message consumption.
   */
  protected AbstractMessagingSubscriberService(TopicId topicId, boolean transactionalFetch, int fetchSize,
                                               long emptyFetchDelayMillis, RetryStrategy retryStrategy,
                                               MetricsContext metricsContext) {
    this.topicId = topicId;
    this.transactionalFetch = transactionalFetch;
    this.fetchSize = fetchSize;
    this.emptyFetchDelayMillis = emptyFetchDelayMillis;
    this.retryStrategy = retryStrategy;
    this.metricsContext = metricsContext;
  }

  /**
   * Returns the {@link TopicId} that this service is fetching from.
   */
  protected final TopicId getTopicId() {
    return topicId;
  }

  /**
   * Returns the {@link MessageContext} that this service used for interacting with TMS.
   */
  protected abstract MessagingContext getMessagingContext();

  /**
   * Returns the {@link Transactional} for executing tasks in transaction.
   */
  protected abstract Transactional getTransactional();

  /**
   * Loads last persisted message id. This method will be called from a transaction.
   * The returned message id will be used as the starting message id (exclusive) for the first fetch.
   *
   * @param datasetContext the {@link DatasetContext} for getting dataset instances.
   * @return the last persisted message id or {@code null} to have first fetch starts from the first available message
   *         in the topic.
   * @throws Exception if failed to load the message id
   */
  @Nullable
  protected abstract String loadMessageId(DatasetContext datasetContext) throws Exception;

  /**
   * Persists the given message id. This method will be called from a transaction, which is the same transaction
   * for the call to {@link #processMessages(DatasetContext, Iterator)}.
   *
   * @param datasetContext the {@link DatasetContext} for getting dataset instances
   * @param messageId the message id that the {@link #processMessages(DatasetContext, Iterator)} has been processed
   *                  up to.
   * @throws Exception if failed to persist the message id
   * @see #processMessages(DatasetContext, Iterator)
   */
  protected abstract void storeMessageId(DatasetContext datasetContext, String messageId) throws Exception;

  /**
   * Decodes the raw {@link Message} into an object of type {@code T}.
   *
   * @param message the {@link Message} to decode
   * @return an object of type {@code T}
   * @throws Exception if the decode failed and the given message will be skipped for processing
   */
  protected abstract T decodeMessage(Message message) throws Exception;

  /**
   * Processes the give list of messages. This method will be called from the same transaction as the
   * {@link #storeMessageId(DatasetContext, String)} call.
   *
   * @param datasetContext the {@link DatasetContext} for getting dataset instances
   * @param messages an {@link Iterator} of {@link ImmutablePair}, with the {@link ImmutablePair#first}
   *                 as the message id, and the {@link ImmutablePair#second} as the decoded message
   * @throws Exception if failed to process the messages
   * @see #storeMessageId(DatasetContext, String)
   */
  protected abstract void processMessages(DatasetContext datasetContext,
                                          Iterator<ImmutablePair<String, T>> messages) throws Exception;

  /**
   * Performs startup task. This method will be called from the executor returned by the {@link #executor()} method.
   * By default this method does nothing.
   */
  protected void doStartUp() {
    // No-op
  }

  /**
   * Performs shutdown task. This method will be called from the executor returned by the {@link #executor()} method.
   * By default this method does nothing.
   */
  protected void doShutdown() {
    // No-op
  }

  /**
   * Perform post processing after a batch of messages has been processed and before the next batch of
   * messages is fetched. This will take place outside of the transaction used when processing messages.
   */
  protected void postProcess() {
    // no-op
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory(getServiceName()));
    return executor;
  }

  @Override
  protected final void startUp() throws Exception {
    doStartUp();
  }

  @Override
  protected final void shutDown() throws Exception {
    try {
      doShutdown();
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  @Override
  protected final void runOneIteration() throws Exception {
    delay = Math.max(0, fetchAndProcessMessages());
    try {
      postProcess();
    } catch (Exception e) {
      LOG.warn("Failed to perform post processing after processing messages.", e);
    }
  }

  @Override
  protected final Scheduler scheduler() {
    return new CustomScheduler() {
      @Override
      protected Schedule getNextSchedule() throws Exception {
        return new Schedule(delay, TimeUnit.MILLISECONDS);
      }
    };
  }

  /**
   * Return the name of this consumer service.
   */
  protected String getServiceName() {
    return getClass().getSimpleName();
  }

  /**
   * The method has the main logic to perform one fetch from TMS and process the fetched messages.
   *
   * @return number of milliseconds to sleep before the next fetch and process should happen.
   */
  private long fetchAndProcessMessages() {
    try {
      if (nonFailureStartTime == 0L) {
        nonFailureStartTime = System.currentTimeMillis();
      }

      // Fetch the messageId if hasn't been fetched
      if (!messageIdInitialized) {
        messageId = Transactionals.execute(getTransactional(), this::loadMessageId);
        messageIdInitialized = true;
      }

      // Collects batch of messages for processing.
      // The fetch may be transactional, and it's ok to have the fetching and the processing happen in two
      // non-overlapping transactions, as long as the processing transaction starts after the fetching one.
      long startTime = System.currentTimeMillis();

      final List<Message> messages = fetchMessages(messageId);
      metricsContext.gauge("tms.fetch.time.ms", System.currentTimeMillis() - startTime);
      metricsContext.increment("tms.fetch.messages", messages.size());

      // Return if stopping or request to sleep for configured number of milliseconds if there are no notifications
      if (messages.isEmpty() || state() != State.RUNNING) {
        return emptyFetchDelayMillis;
      }

      startTime = System.currentTimeMillis();

      // Process the notifications and record the message id of where the processing is up to.
      MessageTrackingIterator iterator = Transactionals.execute(getTransactional(), context -> {
        MessageTrackingIterator trackingIterator = new MessageTrackingIterator(messages.iterator());
        processMessages(context, trackingIterator);
        String lastMessageId = trackingIterator.getLastMessageId();

        // Persist the message id of the last message being consumed from the iterator
        if (lastMessageId != null) {
          storeMessageId(context, lastMessageId);
        }
        return trackingIterator;
      });
      messageId = iterator.getLastMessageId() == null ? messageId : iterator.getLastMessageId();

      long endTime = System.currentTimeMillis();
      metricsContext.gauge("process.duration.ms", endTime - startTime);
      metricsContext.increment("process.notifications", iterator.getConsumedCount());

      // Calculate the delay
      if (messageId != null) {
        metricsContext.gauge("process.delay.ms", endTime - getMessagePublishTime(messageId));
      }

      // Transaction was successful, so reset the failure count and the non failure start time
      failureCount = 0;
      nonFailureStartTime = 0L;
      return 0L;

    } catch (ServiceUnavailableException e) {
      SAMPLING_LOG.warn("Failed to contact service {}. Will retry in next run.", e.getServiceName(), e);
    } catch (TopicNotFoundException e) {
      SAMPLING_LOG.warn("Failed to fetch from TMS. Will retry in next run.", e);
    } catch (Exception e) {
      SAMPLING_LOG.warn("Failed to get and process notifications. Will retry in next run", e);
    }

    // If there is any failure during fetching or processing messages,
    // delay the next fetch based on the strategy
    return retryStrategy.nextRetry(++failureCount, nonFailureStartTime);
  }

  /**
   * Fetch messages from TMS, optionally with transaction.
   */
  private List<Message> fetchMessages(@Nullable final String messageId) throws TopicNotFoundException, IOException {
    if (!transactionalFetch) {
      return doFetchMessages(messageId);
    }
    return Transactionals.execute(getTransactional(), context -> {
      return doFetchMessages(messageId);
    }, TopicNotFoundException.class, IOException.class);
  }

  /**
   * Actually fetching messages from TMS.
   */
  private List<Message> doFetchMessages(@Nullable String messageId) throws TopicNotFoundException, IOException {
    List<Message> messages = new ArrayList<>();
    LOG.trace("Fetching from topic '{}' with messageId '{}'", messageId);
    MessageFetcher messageFetcher = getMessagingContext().getMessageFetcher();
    try (CloseableIterator<Message> iterator = messageFetcher.fetch(topicId.getNamespace(),
                                                                    topicId.getTopic(), fetchSize, messageId)) {
      while (iterator.hasNext() && state() == State.RUNNING) {
        messages.add(iterator.next());
      }
    }
    return messages;
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
   * An {@link Iterator} that decodes {@link Message} and remembers the message id that has
   * been consumed up to.
   */
  private final class MessageTrackingIterator extends AbstractIterator<ImmutablePair<String, T>> {

    private final Iterator<Message> messages;
    private String lastMessageId;
    private int consumedCount;

    MessageTrackingIterator(Iterator<Message> messages) {
      this.messages = messages;
    }

    @Override
    protected ImmutablePair<String, T> computeNext() {
      // Decode the next message into Notification.
      while (messages.hasNext()) {
        consumedCount++;
        Message message = messages.next();
        lastMessageId = message.getId();

        try {
          T decoded = decodeMessage(message);
          LOG.trace("Processing message from topic {} with message id {}: {}", topicId, lastMessageId, decoded);
          return new ImmutablePair<>(lastMessageId, decoded);
        } catch (Exception e) {
          // This shouldn't happen.
          LOG.warn("Failed to decode message with id {} and payload '{}'. Skipped.",
                   message.getId(), message.getPayloadAsString(), e);
        }
      }
      return endOfData();
    }

    @Nullable
    String getLastMessageId() {
      return lastMessageId;
    }

    int getConsumedCount() {
      return consumedCount;
    }
  }
}
