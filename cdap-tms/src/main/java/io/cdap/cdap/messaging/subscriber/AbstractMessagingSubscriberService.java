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

package io.cdap.cdap.messaging.subscriber;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.TimeBoundIterator;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.spi.data.transaction.TxCallable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract base class for implementing message consumption from TMS. This service allows
 * optional transactional fetch from TMS. It always process messages and persisting consumer states
 * in the same transaction.
 *
 * @param <T> the type that each message will be decoded to.
 */
public abstract class AbstractMessagingSubscriberService<T> extends
    AbstractMessagingPollingService<T> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractMessagingSubscriberService.class);

  private final int txTimeoutSeconds;

  /**
   * Specifies tx size for processing fetched messages. If txSize and fetchSize are identical, then
   * all fetched messages are processed in one transaction.
   */
  private final int txSize;

  /**
   * time bound in which fetched messages have to be processed. Message processing will abort of
   * time bound hits.
   */
  private final long timeBoundMillis;

  /**
   * Constructor with txSize being identical to fetchSize.
   *
   * @param topicId the topic to consume from
   * @param fetchSize number of messages to fetch in each batch
   * @param txTimeoutSeconds transaction timeout in seconds to use when processing messages
   * @param emptyFetchDelayMillis number of milliseconds to sleep after a fetch returns empty
   * result
   * @param retryStrategy the {@link RetryStrategy} to determine retry on failure
   * @param metricsContext the {@link MetricsContext} for emitting metrics about the message
   * consumption.
   */
  protected AbstractMessagingSubscriberService(TopicId topicId, int fetchSize,
      int txTimeoutSeconds, long emptyFetchDelayMillis,
      RetryStrategy retryStrategy, MetricsContext metricsContext) {
    this(topicId, fetchSize, txTimeoutSeconds, emptyFetchDelayMillis, retryStrategy, metricsContext,
        fetchSize);

  }

  protected AbstractMessagingSubscriberService(TopicId topicId, int fetchSize,
      int txTimeoutSeconds, long emptyFetchDelayMillis,
      RetryStrategy retryStrategy, MetricsContext metricsContext,
      int txSize) {
    super(topicId, metricsContext, fetchSize, emptyFetchDelayMillis, retryStrategy);
    this.txTimeoutSeconds = txTimeoutSeconds;
    this.txSize = txSize;
    // Process the notifications and record the message id of where the processing is up to.
    // 90% of the tx timeout is .9 * 1000 * txTimeoutSeconds = 900 * txTimeoutSeconds
    this.timeBoundMillis = 900L * txTimeoutSeconds;

  }

  /**
   * Returns the {@link TransactionRunner} for executing tasks in transaction.
   */
  protected abstract TransactionRunner getTransactionRunner();

  /**
   * Loads last persisted message id. This method will be called from a transaction. The returned
   * message id will be used as the starting message id (exclusive) for the first fetch.
   *
   * @param context the {@link StructuredTableContext} for getting dataset instances.
   * @return the last persisted message id or {@code null} to have first fetch starts from the first
   * available message in the topic.
   * @throws Exception if failed to load the message id
   */
  @Nullable
  protected abstract String loadMessageId(StructuredTableContext context) throws Exception;

  /**
   * Persists the given message id. This method will be called from a transaction, which is the same
   * transaction for the call to {@link #processMessages(StructuredTableContext, Iterator)}.
   *
   * @param context the {@link StructuredTableContext} for getting dataset instances
   * @param messageId the message id that the
   * {@link #processMessages(StructuredTableContext, Iterator)} has been processed up to.
   * @throws Exception if failed to persist the message id
   * @see #processMessages(StructuredTableContext, Iterator)
   */
  protected abstract void storeMessageId(StructuredTableContext context, String messageId)
      throws Exception;

  /**
   * Whether the message should run in its own transaction because it is expected to be an expensive
   * operation.
   *
   * @param message the message to process
   * @return whether the message should be processed in its own transaction
   */
  protected boolean shouldRunInSeparateTx(ImmutablePair<String, T> message) {
    return false;
  }

  /**
   * Processes the give list of messages. This method will be called from the same transaction as
   * the {@link #storeMessageId(StructuredTableContext, String)} call. If {@link Exception} is
   * raised from this method, the messages as provided through the {@code messages} parameter will
   * be replayed in the next call.
   *
   * @param structuredTableContext the {@link StructuredTableContext} for getting the tables for the
   * transaction
   * @param messages an {@link Iterator} of {@link ImmutablePair}, with the
   * {@link ImmutablePair#first} as the message id, and the {@link ImmutablePair#second} as the
   * decoded message
   * @throws Exception if failed to process the messages
   * @see #storeMessageId(StructuredTableContext, String)
   */
  protected abstract void processMessages(StructuredTableContext structuredTableContext,
      Iterator<ImmutablePair<String, T>> messages) throws Exception;

  /**
   * Perform post processing after a batch of messages has been processed and before the next batch
   * of messages is fetched. This will take place outside of the transaction used when processing
   * messages.
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

  /**
   * Process messages in multiple transactions where each transaction has size of
   * {@link this#txSize} messages or size of ONE in case
   * {@link this#shouldRunInSeparateTx(ImmutablePair)} returns true For example, for given (m1, m2,
   * m3, m4, m5) with txSize equals 3, and  shouldRunInSeparateTx(m3)=true we process them in the
   * following three transactions (m1, m2) (m3) (m4, m5)
   *
   * @param messages an {@link Iterable} of {@link ImmutablePair}, with the
   * {@link ImmutablePair#first} as the message id, and the {@link ImmutablePair#second} as the
   * decoded message
   */
  @Nullable
  @Override
  protected String processMessages(Iterable<ImmutablePair<String, T>> messages) throws Exception {
    List<ImmutablePair<String, T>> currentTxMessages;
    String lastMessageId = null;
    MessageTrackingIterator iterator = new MessageTrackingIterator(messages.iterator());
    Stopwatch stopwatch = new Stopwatch();
    while (iterator.hasNext()) {
      currentTxMessages = new ArrayList<>();

      while (iterator.hasNext() && currentTxMessages.size() < this.txSize) {
        if (shouldRunInSeparateTx(iterator.peek())) {
          // If next message should run in a separate transaction, two cases exist:
          // 1. if currentTxMessages is empty, we can add next message and go to process it.
          // 2. If currentTxMessages is not empty, we have to process what is in currentTxMessages,
          // and process next message in the next round.
          if (currentTxMessages.isEmpty()) {
            currentTxMessages.add(iterator.next());
          }
          break;
        }
        currentTxMessages.add(iterator.next());
      }

      try {
        String result = processSingleTxn(currentTxMessages, stopwatch);
        if (result != null) {
          // we only set lastMessageId if result of current batch is not null
          // in order to avoid setting lastMessageId to null in case current batch returned null
          // but previous batch finished.
          lastMessageId = result;
        }
      } catch (Exception ex) {
        if (lastMessageId == null) {
          // exception has happened when processing first batch. So we throw the exception
          throw ex;
        }
        LOG.debug(
            "Got exception when processing messages. Last successful processed message Id is {}",
            lastMessageId, ex);
      }
    }

    stopwatch.stop();
    return lastMessageId;
  }

  /**
   * Processes messages in a single transaction. Given that the transaction may be retried due to an
   * exception, messages are of type {@link Iterable} instead of {@link Iterator}. Therefore, inside
   * the transaction, we have to call {@link Iterable#iterator()} every time the transaction is
   * called.
   */
  @Nullable
  private String processSingleTxn(Iterable<ImmutablePair<String, T>> messages, Stopwatch stopwatch)
      throws Exception {

    return TransactionRunners.run(getTransactionRunner(), context -> {
      TimeBoundIterator timeBoundIterator = new TimeBoundIterator<>(messages.iterator(),
          timeBoundMillis, stopwatch);
      MessageTrackingIterator messageTrackingIterator = new MessageTrackingIterator(
          timeBoundIterator);
      processMessages(context, messageTrackingIterator);
      String lastMessageId = messageTrackingIterator.getLastMessageId();

      // Persist the message id of the last message being consumed from the iterator
      if (lastMessageId != null) {
        storeMessageId(context, lastMessageId);
      }
      return lastMessageId;
    }, Exception.class);
  }

  /**
   * An {@link Iterator} that remembers the message id that has been consumed up to.
   */
  private final class MessageTrackingIterator extends AbstractIterator<ImmutablePair<String, T>> {

    Iterator<ImmutablePair<String, T>> iterator;

    private String lastMessageId;

    MessageTrackingIterator(Iterator<ImmutablePair<String, T>> iterator) {
      this.iterator = iterator;
    }

    @Override
    protected ImmutablePair<String, T> computeNext() {
      if (iterator.hasNext()) {
        ImmutablePair<String, T> message = iterator.next();
        lastMessageId = message.getFirst();
        return message;
      }
      return endOfData();
    }

    @Nullable
    String getLastMessageId() {
      return lastMessageId;
    }
  }
}
