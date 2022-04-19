/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.common.collect.Iterators;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

public class AbstractMessagingSubscriberServiceTest {
  @Test
  public void testMesssagingIterator() throws Exception {
    int numMessages = 10;
    List<ImmutablePair<String, String>> messages = createMessages(numMessages);
    DummySubscriberService<String> dummySubscriber = null;

    // Process full message list.
    // Verify that the last
    {
      dummySubscriber = createDummySubscriber(null,
                                              iterator -> {
                                                while (iterator.hasNext()) {
                                                  iterator.next();
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(numMessages - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(numMessages - 1)));
    }

    // Process partial message list.
    {
      final int numProcessed = 5;
      dummySubscriber = createDummySubscriber(null,
                                              iterator -> {
                                                int cnt = 0;
                                                while (iterator.hasNext()) {
                                                  iterator.next();
                                                  cnt++;
                                                  if (cnt >= numProcessed) {
                                                    break;
                                                  }
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(numProcessed - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(numProcessed - 1)));
    }

    // Process partial message list and return after hasNext() before next()
    {
      final int numProcessed = 5;
      dummySubscriber = createDummySubscriber(null,
                                              iterator -> {
                                                int cnt = 0;
                                                while (iterator.hasNext()) {
                                                  if (cnt >= numProcessed) {
                                                    break;
                                                  }
                                                  iterator.next();
                                                  cnt++;
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(numProcessed - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(numProcessed - 1)));
    }

    // Process full message list where the first message requires a separate transaction.
    {
      dummySubscriber = createDummySubscriber(toMessageId(0),
                                              iterator -> {
                                                while (iterator.hasNext()) {
                                                  iterator.next();
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(numMessages - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(numMessages - 1)));
    }

    // Process partial message list since a message in the middle requires separate transaction.
    {
      final int separateTxnMessageId = 4;
      dummySubscriber = createDummySubscriber(toMessageId(separateTxnMessageId),
                                              iterator -> {
                                                while (iterator.hasNext()) {
                                                  iterator.next();
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(separateTxnMessageId - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(separateTxnMessageId - 1)));
    }

    // Process partial message list since the last message requires separate transaction.
    {
      final int separateTxnMessageId = numMessages - 1;
      dummySubscriber = createDummySubscriber(toMessageId(separateTxnMessageId),
                                              iterator -> {
                                                while (iterator.hasNext()) {
                                                  iterator.next();
                                                }
                                                return null;
                                              });
      String lastConsumedId = dummySubscriber.processMessages(Iterators.peekingIterator(messages.iterator()));
      Assert.assertTrue(lastConsumedId != null);
      Assert.assertTrue(lastConsumedId.equals(toMessageId(separateTxnMessageId - 1)));
      Assert.assertTrue(dummySubscriber.getLastStoredMessageId().equals(toMessageId(separateTxnMessageId - 1)));
    }
  }

  private DummySubscriberService createDummySubscriber(@Nullable String separateTxnMessageId,
                                                       Function<Iterator, Void> processMessageFunc) {
    return new DummySubscriberService(new TopicId("default", "dummyTopicId"),
                                      100, 2000, 10,
                                      RetryStrategies.exponentialDelay(1, 5, TimeUnit.SECONDS),
                                      new NoopMetricsContext(), separateTxnMessageId, processMessageFunc);
  }

  private String toMessageId(int messageId) {
    return String.valueOf(messageId);
  }

  private String toMessageBody(int messageId) {
    return String.format("message %d", messageId);
  }

  private List<ImmutablePair<String, String>> createMessages(int numMessages) {
    List<ImmutablePair<String, String>> messages = new ArrayList<>();
    for (int i = 0; i < numMessages; i++) {
      messages.add(new ImmutablePair<>(toMessageId(i), toMessageBody(i)));
    }
    return messages;
  }

  private class DummySubscriberService<T> extends AbstractMessagingSubscriberService {
    Function<Iterator, Void> processMessageFunc;
    String separateTransactionMessageId;
    String lastStoredMessageId;

    protected DummySubscriberService(TopicId topicId, int fetchSize, long emptyFetchDelayMillis,
                                     int txTimeoutSeconds, RetryStrategy retryStrategy,
                                     MetricsContext metricsContext, String separateTransactionMessageId,
                                     Function<Iterator, Void> processMessageFunc) {
      super(topicId, fetchSize, txTimeoutSeconds, emptyFetchDelayMillis, retryStrategy, metricsContext);
      this.separateTransactionMessageId = separateTransactionMessageId;
      this.processMessageFunc = processMessageFunc;
    }

    @Nullable
    String getLastStoredMessageId() {
      return lastStoredMessageId;
    }

    @Override
    protected MessagingContext getMessagingContext() {
      return null;
    }

    @Override
    protected T decodeMessage(Message message) {
      return null;
    }

    @Override
    protected TransactionRunner getTransactionRunner() {
      return runnable -> {
        try {
          runnable.run(null);
        } catch (Exception e) {
          throw new TransactionException(e.getMessage(), e);
        }
      };
    }

    @Nullable
    @Override
    protected java.lang.String loadMessageId(StructuredTableContext context) {
      return null;
    }

    @Override
    protected void storeMessageId(StructuredTableContext context, String messageId) {
      lastStoredMessageId = messageId;
    }

    @Override
    protected void processMessages(StructuredTableContext structuredTableContext, Iterator messages) {
      processMessageFunc.apply(messages);
    }

    @Override
    protected boolean shouldRunInSeparateTx(ImmutablePair message) {
      if (separateTransactionMessageId == null) {
        return false;
      }
      return message.getFirst().equals(separateTransactionMessageId);
    }
  }
}
