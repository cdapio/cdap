/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.messaging;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.Message;
import co.cask.cdap.api.messaging.MessageFetcher;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * An app containing programs for testing TMS interactions
 */
public class MessagingApp extends AbstractApplication {

  static final String TOPIC = "topic";
  static final String CONTROL_TOPIC = "controlTopic";

  @Override
  public void configure() {
    addWorker(new MessagingWorker());
    addWorker(new TransactionalMessagingWorker());
    addSpark(new MessagingSpark());
  }

  /**
   * Fetch and block until it get a message.
   */
  private static Message fetchMessage(MessageFetcher fetcher, String namespace, String topic,
                                      @Nullable String afterMessageId, long timeout, TimeUnit unit) throws Exception {
    CloseableIterator<Message> iterator = fetcher.fetch(namespace, topic, 1, afterMessageId);
    Stopwatch stopwatch = new Stopwatch().start();
    try {
      while (!iterator.hasNext() && stopwatch.elapsedTime(unit) < timeout) {
        TimeUnit.MILLISECONDS.sleep(100);
        iterator = fetcher.fetch(namespace, topic, 1, afterMessageId);
      }

      if (!iterator.hasNext()) {
        throw new TimeoutException("Failed to get any messages from " + topic +
                                     " in " + timeout + " " + unit.name().toLowerCase());
      }
      // The payload contains the message to publish in next step
      return iterator.next();
    } finally {
      iterator.close();
    }
  }

  /**
   * A worker for testing both transactional and non-transactional aspect of {@link MessagingContext}.
   */
  public static final class MessagingWorker extends AbstractWorker {

    @Override
    public void run() {
      try {
        // Create a topic
        getContext().getAdmin().createTopic(TOPIC);

        // Fetch a message published from test driver
        final MessageFetcher fetcher = getContext().getMessageFetcher();
        Message message = fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 3, TimeUnit.SECONDS);

        // Publish the message
        final MessagePublisher publisher = getContext().getMessagePublisher();
        String payload = message.getPayloadAsString();
        publisher.publish(getContext().getNamespace(), TOPIC, payload + payload);

        final AtomicReference<String> lastMessageId = new AtomicReference<>(message.getId());

        // Execute publish and fetch in a transaction
        getContext().execute(new TxRunnable() {
          @Override
          public void run(DatasetContext context) throws Exception {
            // Fetch a message again, the test driver should published one
            Message message = fetchMessage(fetcher, getContext().getNamespace(), TOPIC,
                                           lastMessageId.get(), 3, TimeUnit.SECONDS);
            lastMessageId.set(message.getId());

            // Publish the message transactionally.
            String payload = message.getPayloadAsString();
            publisher.publish(getContext().getNamespace(), TOPIC, payload + payload);

            // Block until the test driver publish a message to control topic
            fetchMessage(fetcher, getContext().getNamespace(), CONTROL_TOPIC, null, 10, TimeUnit.SECONDS);
          }
        });

      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * A worker for testing transactional aspect of {@link MessagingContext}.
   */
  public static final class TransactionalMessagingWorker extends AbstractWorker {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalMessagingWorker.class);

    @Override
    public void run() {
      // This boolean tells if getting the MessagePublisher/MessageFetcher inside tx or outside
      final boolean getInTx = Boolean.valueOf(getContext().getRuntimeArguments().get("get.in.tx"));
      final String payload = "payload";
      final AtomicBoolean succeeded = new AtomicBoolean();
      final CountDownLatch publishedLatch = new CountDownLatch(1);
      final CountDownLatch publishCommitLatch = new CountDownLatch(1);

      // Create two threads, one publish, one consumer, both transactionally.
      Thread publishThread = new Thread(getClass().getSimpleName() + "-publish") {
        @Override
        public void run() {
          try {
            final AtomicReference<MessagePublisher> publisherRef = new AtomicReference<>();
            if (!getInTx) {
              // Get a publisher before starting tx. When the tx starts, it should get propagated into the publisher.
              LOG.info("Get publisher outside of TX");
              publisherRef.set(getContext().getMessagePublisher());
            }

            // Publish a message transactionally. Block until signaled before committing.
            getContext().execute(new TxRunnable() {
              @Override
              public void run(DatasetContext context) throws Exception {
                MessagePublisher publisher = publisherRef.get();
                if (publisher == null) {
                  LOG.info("Get publisher inside of TX");
                  publisher = getContext().getMessagePublisher();
                }
                publisher.publish(getContext().getNamespace(), TOPIC, payload);
                LOG.info("Message published");
                publishedLatch.countDown();
                publishCommitLatch.await();
              }
            });
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
      Thread fetchThread = new Thread(getClass().getSimpleName() + "-fetch") {
        @Override
        public void run() {
          final AtomicReference<MessageFetcher> fetcherRef = new AtomicReference<>();
          if (!getInTx) {
            // Get a fetcher before starting tx. When the tx starts, it should get propagated into the fetcher
            LOG.info("Get fetcher outside of TX");
            fetcherRef.set(getContext().getMessageFetcher());
          }

          try {
            // Wait until published before starting the fetcher transaction
            publishedLatch.await();
            getContext().execute(new TxRunnable() {
              @Override
              public void run(DatasetContext context) throws Exception {
                MessageFetcher fetcher = fetcherRef.get();
                if (fetcher == null) {
                  LOG.info("Get fetcher inside of TX");
                  fetcher = getContext().getMessageFetcher();
                }
                // Should get nothing because publish tx hasn't been committed
                try {
                  fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 3, TimeUnit.SECONDS);
                  throw new IllegalStateException("Expected timeout");
                } catch (TimeoutException e) {
                  LOG.info("Expected timeout exception raised");
                }
                // Signal publish tx to commit
                publishCommitLatch.countDown();

                // Even after publish, we won't see the message, since the message should still excluded
                try {
                  fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 3, TimeUnit.SECONDS);
                  throw new IllegalStateException("Expected timeout");
                } catch (TimeoutException e) {
                  LOG.info("Expected timeout exception raised");
                }
              }
            });


            // Start a new transaction, should be able to see the new message
            getContext().execute(new TxRunnable() {
              @Override
              public void run(DatasetContext context) throws Exception {
                MessageFetcher fetcher = fetcherRef.get();
                if (fetcher == null) {
                  LOG.info("Get fetcher inside of TX");
                  fetcher = getContext().getMessageFetcher();
                }
                // Now should be able to fetch a message
                Message message = fetchMessage(fetcher, getContext().getNamespace(), TOPIC, null, 3, TimeUnit.SECONDS);
                LOG.info("Message fetched {}", message);
                succeeded.set(payload.equals(message.getPayloadAsString()));
              }
            });
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

      publishThread.start();
      fetchThread.start();
      Uninterruptibles.joinUninterruptibly(publishThread);
      Uninterruptibles.joinUninterruptibly(fetchThread);
      Preconditions.checkState(succeeded.get(), "Expected succeeded to be true.");
    }
  }
}
