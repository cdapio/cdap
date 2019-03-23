/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.common.utils.TimeBoundIterator;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.cdap.spi.data.transaction.TxCallable;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * An abstract base class for implementing message consumption from TMS.
 * This service allows optional transactional fetch from TMS.
 * It always process messages and persisting consumer states in the same transaction.
 *
 * @param <T> the type that each message will be decoded to.
 */
public abstract class AbstractMessagingSubscriberService<T> extends AbstractMessagingPollingService<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMessagingSubscriberService.class);

  private final int txTimeoutSeconds;

  /**
   * Constructor.
   *
   * @param topicId the topic to consume from
   * @param fetchSize number of messages to fetch in each batch
   * @param txTimeoutSeconds transaction timeout in seconds to use when processing messages
   * @param emptyFetchDelayMillis number of milliseconds to sleep after a fetch returns empty result
   * @param retryStrategy the {@link RetryStrategy} to determine retry on failure
   * @param metricsContext the {@link MetricsContext} for emitting metrics about the message consumption.
   */
  protected AbstractMessagingSubscriberService(TopicId topicId, int fetchSize,
                                               int txTimeoutSeconds, long emptyFetchDelayMillis,
                                               RetryStrategy retryStrategy, MetricsContext metricsContext) {
    super(topicId, metricsContext, fetchSize, emptyFetchDelayMillis, retryStrategy);
    this.txTimeoutSeconds = txTimeoutSeconds;
  }

  /**
   * Returns the {@link TransactionRunner} for executing tasks in transaction.
   */
  protected abstract TransactionRunner getTransactionRunner();

  /**
   * Loads last persisted message id. This method will be called from a transaction.
   * The returned message id will be used as the starting message id (exclusive) for the first fetch.
   *
   * @param context the {@link StructuredTableContext} for getting dataset instances.
   * @return the last persisted message id or {@code null} to have first fetch starts from the first available message
   *         in the topic.
   * @throws Exception if failed to load the message id
   */
  @Nullable
  protected abstract String loadMessageId(StructuredTableContext context) throws Exception;

  /**
   * Persists the given message id. This method will be called from a transaction, which is the same transaction
   * for the call to {@link #processMessages(StructuredTableContext, Iterator)}.
   *
   * @param context the {@link StructuredTableContext} for getting dataset instances
   * @param messageId the message id that the {@link #processMessages(StructuredTableContext, Iterator)}
   *                  has been processed
   *                  up to.
   * @throws Exception if failed to persist the message id
   * @see #processMessages(StructuredTableContext, Iterator)
   */
  protected abstract void storeMessageId(StructuredTableContext context, String messageId) throws Exception;

  /**
   * Whether the message should run in its own transaction because it is expected to be an expensive operation.
   *
   * @param message the message to process
   * @return whether the message should be processed in its own transaction
   */
  protected boolean shouldRunInSeparateTx(ImmutablePair<String, T> message) {
    return false;
  }

  /**
   * Processes the give list of messages. This method will be called from the same transaction as the
   * {@link #storeMessageId(StructuredTableContext, String)} call. If {@link Exception} is raised from this method,
   * the messages as provided through the {@code messages} parameter will be replayed in the next call.
   *
   * @param structuredTableContext the {@link StructuredTableContext} for getting the tables for the transaction
   * @param messages an {@link Iterator} of {@link ImmutablePair}, with the {@link ImmutablePair#first}
   *                 as the message id, and the {@link ImmutablePair#second} as the decoded message
   * @throws Exception if failed to process the messages
   * @see #storeMessageId(StructuredTableContext, String)
   */
  protected abstract void processMessages(StructuredTableContext structuredTableContext,
                                          Iterator<ImmutablePair<String, T>> messages) throws Exception;

  /**
   * Perform post processing after a batch of messages has been processed and before the next batch of
   * messages is fetched. This will take place outside of the transaction used when processing messages.
   */
  @Override
  protected void postProcess() {
    // no-op
  }

  @Nullable
  @Override
  protected final String loadMessageId() {
    return TransactionRunners.run(getTransactionRunner(), (TxCallable<String>) this::loadMessageId);
  }

  @Nullable
  @Override
  protected String processMessages(Iterator<ImmutablePair<String, T>> messages) throws Exception {
    MessageTrackingIterator iterator;

    // Process the notifications and record the message id of where the processing is up to.
    // 90% of the tx timeout is .9 * 1000 * txTimeoutSeconds = 900 * txTimeoutSeconds
    long timeBoundMillis = 900L * txTimeoutSeconds;
    iterator = TransactionRunners.run(getTransactionRunner(), context -> {
      TimeBoundIterator<ImmutablePair<String, T>> timeBoundMessages = new TimeBoundIterator<>(messages,
                                                                                              timeBoundMillis);
      MessageTrackingIterator trackingIterator = new MessageTrackingIterator(timeBoundMessages);
      processMessages(context, trackingIterator);
      String lastMessageId = trackingIterator.getLastMessageId();

      // Persist the message id of the last message being consumed from the iterator
      if (lastMessageId != null) {
        storeMessageId(context, lastMessageId);
      }
      return trackingIterator;
    }, Exception.class);

    return iterator.getLastMessageId();
  }

  /**
   * An {@link Iterator} that remembers the message id that has been consumed up to.
   */
  private final class MessageTrackingIterator extends AbstractIterator<ImmutablePair<String, T>> {

    private final Iterator<ImmutablePair<String, T>> messages;
    private String lastMessageId;
    private int consumedCount;
    private boolean shouldEnd;

    MessageTrackingIterator(Iterator<ImmutablePair<String, T>> messages) {
      this.messages = messages;
      this.consumedCount = 0;
      this.shouldEnd = false;
    }

    @Override
    protected ImmutablePair<String, T> computeNext() {
      if (shouldEnd || !messages.hasNext()) {
        return endOfData();
      }

      ImmutablePair<String, T> message = messages.next();
      if (shouldRunInSeparateTx(message)) {
        // if we should process this message in a separate tx and we've already processed other messages,
        // pretend we've gone through all messages already. The next time we try to process a batch of messages,
        // this expensive one will be the first message.
        if (consumedCount > 0) {
          LOG.debug("Ending message batch early to process {} in a separate tx", message.getSecond());
          return endOfData();
        }
        // if we should process this message in a separate tx and we haven't processed any messages yet,
        // remember that we should pretend this iterator only had one element in it
        shouldEnd = true;
      }
      consumedCount++;
      lastMessageId = message.getFirst();
      return message;
    }

    @Nullable
    String getLastMessageId() {
      return lastMessageId;
    }
  }
}
