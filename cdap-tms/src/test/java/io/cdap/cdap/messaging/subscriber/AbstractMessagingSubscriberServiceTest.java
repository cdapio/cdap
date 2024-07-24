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

package io.cdap.cdap.messaging.subscriber;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AbstractMessagingSubscriberServiceTest {

  @Rule
  public MockitoRule mockitorule = MockitoJUnit.rule();

  @Mock
  private MessageFetcher messageFetcher;

  @Mock
  private MessagingContext messagingContext;

  @Mock
  private TransactionRunner transactionRunner;

  @Mock
  private MetricsContext metricsContext;

  @Mock
  private Message message0;

  @Mock
  private Message message1;

  @Mock
  private Message message2;

  @Mock
  private Message message3;

  @Mock
  private Message message4;

  @Mock
  private Message message5;

  @Mock
  private Message message6;

  @Mock
  private StructuredTableContext structuredTableContext;

  private String storedMessageId = "start";
  private BlockingQueue<List<ImmutablePair<String, Integer>>> processedMessages = new ArrayBlockingQueue<>(10);

  @Test
  public void testTransactionRetry()
    throws TopicNotFoundException, IOException, TransactionException, InterruptedException {
    Iterator<Message> messages = Arrays.asList(message0, message1).iterator();
    Mockito.when(messagingContext.getMessageFetcher()).thenReturn(messageFetcher);
    Mockito.when(messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), "test", 100, "start"))
      .thenReturn(new AbstractCloseableIterator<Message>() {
        @Override
        protected Message computeNext() {
          return messages.hasNext() ? messages.next() : endOfData();
        }

        @Override
        public void close() {

        }
      });
    Mockito.doAnswer(c -> {
      //First one
      c.getArgument(0, TxRunnable.class).run(structuredTableContext);
      //Retry
      c.getArgument(0, TxRunnable.class).run(structuredTableContext);
      return null;
    }).when(transactionRunner).run(Mockito.any());

    TestMessagingSubscriberService service = new TestMessagingSubscriberService(
      NamespaceId.DEFAULT.topic("test"), 100, 100, 1,
      RetryStrategies.noRetry(), metricsContext, 100);
    service.startAndWait();
    //First one
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 0), ImmutablePair.of(null, 1)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    //Retry
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 0), ImmutablePair.of(null, 1)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  @Test
  public void testTransactionBatching()
    throws TopicNotFoundException, IOException, TransactionException, InterruptedException {

    Iterator<Message> messages =
      Arrays.asList(message0, message1, message2, message3, message4, message5, message6)
      .iterator();
    Mockito.when(messagingContext.getMessageFetcher()).thenReturn(messageFetcher);
    Mockito.when(messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), "test", 100, "start"))
      .thenReturn(new AbstractCloseableIterator<Message>() {
        @Override
        protected Message computeNext() {
          return messages.hasNext() ? messages.next() : endOfData();
        }

        @Override
        public void close() {

        }
      });
    Mockito.doAnswer(c -> {
      c.getArgument(0, TxRunnable.class).run(structuredTableContext);
      return null;
    }).when(transactionRunner).run(Mockito.any());

    TestMessagingSubscriberService service = new TestMessagingSubscriberService(
      NamespaceId.DEFAULT.topic("test"), 100, 100, 1,
      RetryStrategies.noRetry(), metricsContext, 3);
    service.startAndWait();
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 0), ImmutablePair.of(null, 1), ImmutablePair.of(null, 2)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 3)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 4)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 5), ImmutablePair.of(null, 6)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  /**
   * Test time bound transaction batching by forcing 3sec sleep inside transaction and setting txTimeoutSeconds to 2sec
   */
  @Test
  public void testTimeBoundTransactionBatching()
    throws TopicNotFoundException, IOException, TransactionException, InterruptedException {

    Iterator<Message> messages =
      Arrays.asList(message0, message1, message2, message3, message4)
        .iterator();
    Mockito.when(messagingContext.getMessageFetcher()).thenReturn(messageFetcher);
    Mockito.when(messageFetcher.fetch(NamespaceId.DEFAULT.getNamespace(), "test", 100, "start"))
      .thenReturn(new AbstractCloseableIterator<Message>() {
        @Override
        protected Message computeNext() {
          return messages.hasNext() ? messages.next() : endOfData();
        }

        @Override
        public void close() {

        }
      });
    Mockito.doAnswer(c -> {
      c.getArgument(0, TxRunnable.class).run(structuredTableContext);
      Thread.sleep(3000);
      return null;
    }).when(transactionRunner).run(Mockito.any());

    TestMessagingSubscriberService service = new TestMessagingSubscriberService(
      NamespaceId.DEFAULT.topic("test"), 100, 2, 1,
      RetryStrategies.noRetry(), metricsContext, 2);
    service.startAndWait();
    Assert.assertEquals(Arrays.asList(ImmutablePair.of(null, 0), ImmutablePair.of(null, 1)),
                        processedMessages.poll(10, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  class TestMessagingSubscriberService extends AbstractMessagingSubscriberService<Integer> {
    private int counter;

    protected TestMessagingSubscriberService(TopicId topicId, int fetchSize,
                                             int txTimeoutSeconds, long emptyFetchDelayMillis,
                                             RetryStrategy retryStrategy, MetricsContext metricsContext,
                                             int txSize) {
      super(topicId, fetchSize, txTimeoutSeconds, emptyFetchDelayMillis, retryStrategy, metricsContext, txSize);
    }

    @Override
    protected MessagingContext getMessagingContext() {
      return messagingContext;
    }

    @Override
    protected Integer decodeMessage(Message message) throws Exception {
      return counter++;
    }

    @Override
    protected TransactionRunner getTransactionRunner() {
      return transactionRunner;
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) throws Exception {
      return storedMessageId;
    }

    @Override
    protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
      storedMessageId = messageId;
    }

    @Override
    protected void processMessages(StructuredTableContext structuredTableContext,
                                   Iterator<ImmutablePair<String, Integer>> messages) throws Exception {
      processedMessages.add(ImmutableList.copyOf(messages));
    }

    protected boolean shouldRunInSeparateTx(ImmutablePair<String, Integer> message) {
      if (message.getSecond() == 4) {
        return true;
      }
      return false;
    }

  }

}
