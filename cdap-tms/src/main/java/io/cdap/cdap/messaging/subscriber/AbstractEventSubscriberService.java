package io.cdap.cdap.messaging.subscriber;

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

import com.google.common.collect.AbstractIterator;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.common.utils.TimeBoundIterator;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * An abstract base class for implementing event deletion message consumption from TMS.
 *
 * @param <T> the type that each message will be decoded to.
 */
public abstract class AbstractEventSubscriberService<T> extends AbstractMessagingPollingService<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractEventSubscriberService.class);

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
  protected AbstractEventSubscriberService(TopicId topicId, int fetchSize,
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
   * Whether the message should run in its own transaction because it is expected to be an expensive operation.
   *
   * @param message the message to process
   * @return whether the message should be processed in its own transaction
   */
  protected boolean shouldRunInSeparateTx(ImmutablePair<String, T> message) {
    return false;
  }

  /**
   * Processes the give list of messages. If {@link Exception} is raised from this method,
   * the messages as provided through the {@code messages} parameter will be replayed in the next call.
   *
   * @param messages an {@link Iterator} of {@link ImmutablePair}, with the {@link ImmutablePair#first}
   *                 as the message id, and the {@link ImmutablePair#second} as the decoded message
   * @throws Exception if failed to process the messages
   */
  protected abstract void processMessages(Iterator<ImmutablePair<String, T>> messages) throws Exception;

  @Nullable
  @Override
  protected String loadMessageId() throws IOException {
    //no-op
    return null;
  }

  @Nullable
  @Override
  protected String processMessages(Iterable<ImmutablePair<String, T>> messages) throws Exception {
    AbstractEventSubscriberService.MessageTrackingIterator iterator;

    // Process the notifications and record the message id of where the processing is up to.
    // 90% of the tx timeout is .9 * 1000 * txTimeoutSeconds = 900 * txTimeoutSeconds
    long timeBoundMillis = 900L * txTimeoutSeconds;
    iterator = TransactionRunners.run(getTransactionRunner(), context -> {
      TimeBoundIterator<ImmutablePair<String, T>> timeBoundMessages = new TimeBoundIterator<>(messages.iterator(),
                                                                                              timeBoundMillis);
      AbstractEventSubscriberService.MessageTrackingIterator trackingIterator = new AbstractEventSubscriberService
        .MessageTrackingIterator(timeBoundMessages);
      processMessages(trackingIterator);

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

