/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications.kafka;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.notifications.NotificationContext;
import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationHandler;
import co.cask.cdap.notifications.NotificationSubscriber;
import co.cask.cdap.notifications.TxRetryPolicy;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.service.NotificationFeedException;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Kafka implementation of a {@link co.cask.cdap.notifications.NotificationPublisher}.
 */
public class KafkaNotificationSubscriber implements NotificationSubscriber {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationSubscriber.class);

  private final KafkaClient kafkaClient;
  private final DatasetFramework dsFramework;
  private final NotificationFeedClient feedClient;
  private final TransactionSystemClient transactionSystemClient;

  @Inject
  public KafkaNotificationSubscriber(KafkaClient kafkaClient, DatasetFramework dsFramework,
                                     NotificationFeedClient feedClient,
                                     TransactionSystemClient transactionSystemClient) {
    this.kafkaClient = kafkaClient;
    this.dsFramework = dsFramework;
    this.feedClient = feedClient;
    this.transactionSystemClient = transactionSystemClient;
  }

  @Override
  public Preparer prepare() {
    final KafkaConsumer.Preparer kafkaPreparer = kafkaClient.getConsumer().prepare();
    final Map<NotificationFeed, NotificationHandler> feedMap = Maps.newConcurrentMap();

    return new Preparer() {
      private boolean isConsuming = false;

      @Override
      public synchronized <N> Preparer add(NotificationFeed feed, NotificationHandler<N> handler)
        throws NotificationFeedException {
        if (isConsuming) {
          throw new NotificationFeedException("Preparer is already consuming Notifications, feeds cannot be added.");
        }

        // This call will make sure that the feed exists
        feedClient.getFeed(feed);

        String topic = KafkaNotificationUtils.getKafkaTopic(feed);

        // TODO there is a bug in twill, that when the topic doesn't exist, add latest will not make subscription
        // start from offset 0 - but that will be fixed soon
        kafkaPreparer.addLatest(topic, 0);

        feedMap.put(feed, handler);
        return this;
      }

      @Override
      public synchronized Cancellable consume() {
        isConsuming = true;
        return kafkaPreparer.consume(new KafkaConsumer.MessageCallback() {
          @Override
          public void onReceived(Iterator<FetchedMessage> messages) {
            int count = 0;
            while (messages.hasNext()) {
              FetchedMessage message = messages.next();
              ByteBuffer payload = message.getPayload();

              try {
                String msgKey = KafkaMessageSerializer.decodeMessageKey(payload);
                for (Map.Entry<NotificationFeed, NotificationHandler> feedEntry : feedMap.entrySet()) {
                  if (!msgKey.equals(KafkaMessageSerializer.buildKafkaMessageKey(feedEntry.getKey()))) {
                    continue;
                  }
                  Object notification = KafkaMessageSerializer.decode(payload,
                                                                      feedEntry.getValue().getNotificationFeedType());
                  if (notification == null) {
                    continue;
                  }
                  try {
                    feedEntry.getValue().processNotification(notification, new BasicNotificationContext());
                    count++;
                  } catch (Throwable t) {
                    LOG.warn("Error while processing notification: {}", notification, t);
                  }
                }
              } catch (IOException e) {
                LOG.error("Could not decode Kafka message {} using Gson. Make sure that the " +
                            "getNotificationFeedType() method is correctly set.", message, e);
              }
            }
            LOG.debug("Successfully handled {} messages from kafka", count);
          }

          @Override
          public void finished() {
            LOG.info("Subscription finished.");
          }
        });
      }
    };
  }

  /**
   * Implementation of {@link co.cask.cdap.notifications.NotificationContext}.
   */
  private final class BasicNotificationContext implements NotificationContext {

    @Override
    public boolean execute(TxRunnable runnable, TxRetryPolicy policy) {
      int countFail = 0;
      while (true) {
        try {
          final TransactionContext context = new TransactionContext(transactionSystemClient);
          try {
            context.start();
            runnable.run(new DynamicDatasetContext(context));
            context.finish();
            return true;
          } catch (TransactionFailureException e) {
            abortTransaction(e, "Failed to commit. Aborting transaction.", context);
          } catch (Exception e) {
            abortTransaction(e, "Exception occurred running user code. Aborting transaction.", context);
          }
        } catch (Throwable t) {
          switch (policy.handleFailure(++countFail, t)) {
            case RETRY:
              LOG.warn("Retrying failed transaction");
              break;
            case DROP:
              LOG.warn("Could not execute transactional operation.", t);
              return false;
          }
        }
      }
    }

    private void abortTransaction(Exception e, String message, TransactionContext context) {
      try {
        LOG.error(message, e);
        context.abort();
        throw Throwables.propagate(e);
      } catch (TransactionFailureException e1) {
        LOG.error("Failed to abort transaction.", e1);
        throw Throwables.propagate(e1);
      }
    }
  }

  /**
   * Implementation of {@link co.cask.cdap.api.data.DatasetContext} that allows to dynamically load datasets
   * into a started {@link TransactionContext}.
   */
  private final class DynamicDatasetContext implements DatasetContext {

    private final TransactionContext context;

    private DynamicDatasetContext(TransactionContext context) {
      this.context = context;
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(name, null);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      try {
        T dataset = dsFramework.getDataset(name, arguments, null);
        if (dataset instanceof TransactionAware) {
          context.addTransactionAware((TransactionAware) dataset);
        }
        return dataset;
      } catch (DatasetManagementException e) {
        throw new DatasetInstantiationException("Could not retrieve dataset metadata", e);
      } catch (IOException e) {
        throw new DatasetInstantiationException("Error when instantiating dataset", e);
      }
    }
  }
}
