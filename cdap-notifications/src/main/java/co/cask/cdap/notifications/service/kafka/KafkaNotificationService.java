/*
* Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.notifications.service.kafka;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.AbstractNotificationService;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Kafka implementation of the {@link NotificationService}.
 */
public class KafkaNotificationService extends AbstractNotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationService.class);
  private static final Gson GSON = new GsonBuilder()
    .enableComplexMapKeySerialization()
    .create();

  private final KafkaClient kafkaClient;
  private final NotificationFeedManager feedManager;
  private final KafkaPublisher.Ack ack;

  private final Map<TopicPartition, KafkaNotificationsCallback> kafkaCallbacks;
  private KafkaPublisher kafkaPublisher;
  private final int nbPartitions;

  // Executor to publish notifications to Kafka
  private ListeningExecutorService publishingExecutor;

  @Inject
  public KafkaNotificationService(CConfiguration cConf, KafkaClient kafkaClient, DatasetFramework dsFramework,
                                  TransactionSystemClient transactionSystemClient,
                                  NotificationFeedManager feedManager) {
    super(dsFramework, transactionSystemClient, feedManager);
    this.kafkaClient = kafkaClient;
    this.feedManager = feedManager;
    this.ack = KafkaPublisher.Ack.LEADER_RECEIVED;
    this.nbPartitions = cConf.getInt("kafka.num.partitions");

    this.kafkaCallbacks = Maps.newHashMap();
  }

  @Override
  protected void startUp() throws Exception {
    kafkaPublisher = kafkaClient.getPublisher(ack, Compression.SNAPPY);
    publishingExecutor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-publisher-%d")));
  }

  @Override
  protected void shutDown() throws Exception {
    publishingExecutor.shutdownNow();
  }

  @Override
  public <N> ListenableFuture<N> publish(final Id.NotificationFeed feed, final N notification,
                                         final Type notificationType)
    throws NotificationException {
    LOG.trace("Publishing on notification feed [{}]: {}", feed, notification);
    return publishingExecutor.submit(new Callable<N>() {
      @Override
      public N call() throws Exception {
        try {
          KafkaMessage message = new KafkaMessage(KafkaNotificationUtils.getMessageKey(feed),
                                                  GSON.toJsonTree(notification, notificationType));
          ByteBuffer bb = KafkaMessageCodec.encode(message);

          TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);
          KafkaPublisher.Preparer preparer = kafkaPublisher.prepare(topicPartition.getTopic());
          preparer.add(bb, message.getMessageKey());

          try {
            preparer.send().get();
            return notification;
          } catch (ExecutionException e) {
            throw new NotificationException(e.getCause());
          }
        } catch (IOException e) {
          throw new NotificationException(e);
        }
      }
    });
  }

  @Override
  public <N> Cancellable subscribe(Id.NotificationFeed feed, NotificationHandler<N> handler,
                                   Executor executor)
    throws NotificationFeedNotFoundException, NotificationFeedException {
    // This call will make sure that the feed exists
    feedManager.getFeed(feed);

    final TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);

    synchronized (this) {
      KafkaNotificationsCallback kafkaCallback = kafkaCallbacks.get(topicPartition);
      if (kafkaCallback == null) {
        LOG.debug("Creating new Kafka notification callback for topic-partition {}", topicPartition);
        kafkaCallback = new KafkaNotificationsCallback(topicPartition);
        kafkaCallbacks.put(topicPartition, kafkaCallback);
      }
      return kafkaCallback.subscribe(feed, handler, executor);
    }
  }

  /**
   * Callback class called when a Kafka message is received. The {@link #onReceived} method will
   * extract the feed ID of the message received, and pass the notification encoded in the message
   * to all handlers that are interested in that feed, using the {@code delegate} in-memory notification
   * service.
   * One callback is created per TopicPartition. It is created by the first subscription to a feed
   * which maps to the TopicPartition.
   */
  private final class KafkaNotificationsCallback implements KafkaConsumer.MessageCallback {

    private final TopicPartition topicPartition;
    private int subscriptions;

    private Cancellable kafkaSubscription;

    private KafkaNotificationsCallback(TopicPartition topicPartition) {
      this.topicPartition = topicPartition;
    }

    public <N> Cancellable subscribe(Id.NotificationFeed feed, NotificationHandler<N> handler,
                                     Executor executor)
      throws NotificationFeedNotFoundException, NotificationFeedException {
      final Cancellable cancellable = KafkaNotificationService.super.subscribe(feed, handler, executor);
      synchronized (KafkaNotificationService.this) {
        if (subscriptions == 0) {
          KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

          for (int i = 0; i < nbPartitions; i++) {
            // TODO there is a bug in twill, that when the topic doesn't exist, add latest will not make subscription
            // start from offset 0 - but that will be fixed soon

            // For now, subscribe to all the partitions, because we don't know exactly to what
            // partition the feed will be mapped to. Twill does not expose the possibility to choose
            // a partitioner.
            preparer.addLatest(topicPartition.getTopic(), i);
          }
          kafkaSubscription = preparer.consume(this);
        }
        subscriptions++;
      }
      return new Cancellable() {

        @Override
        public void cancel() {
          cancellable.cancel();
          synchronized (KafkaNotificationService.this) {
            subscriptions--;
            if (subscriptions == 0) {
              kafkaSubscription.cancel();
              kafkaCallbacks.remove(topicPartition);
            }
          }
        }
      };
    }

    @Override
    public void onReceived(Iterator<FetchedMessage> messages) {
      int count = 0;
      while (messages.hasNext()) {
        count++;
        FetchedMessage message = messages.next();
        ByteBuffer payload = message.getPayload();

        try {
          KafkaMessage decodedMessage = KafkaMessageCodec.decode(payload);
          try {
            LOG.trace("Decoded notification from Kafka: {}", decodedMessage);
            notificationReceived(KafkaNotificationUtils.getMessageFeed(decodedMessage.getMessageKey()),
                                 decodedMessage.getNotificationJson());
          } catch (Throwable t) {
            LOG.warn("Error while processing notification {} with handler {}",
                     decodedMessage.getNotificationJson(), t);
          }
        } catch (IOException e) {
          LOG.error("Could not decode Kafka message {} using Gson.", message, e);
        }
      }
      LOG.debug("Handled {} messages from kafka", count);
    }

    @Override
    public void finished() {
      LOG.info("Subscription to topic partition {} finished.", topicPartition);
    }
  }
}
