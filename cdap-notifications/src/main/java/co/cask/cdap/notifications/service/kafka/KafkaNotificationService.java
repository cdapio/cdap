/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.notifications.service.inmemory.InMemoryNotificationService;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Kafka implementation of the {@link NotificationService}.
 */
public class KafkaNotificationService extends AbstractIdleService implements NotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationService.class);
  private static final Gson GSON = new Gson();

  private static final int KAFKA_PUBLISHERS_CACHE_TIMEOUT = 3600;

  private final KafkaClient kafkaClient;
  private final NotificationFeedManager feedManager;
  private final KafkaPublisher.Ack ack;
  private final InMemoryNotificationService delegate;

  // Cached instances of KafkaPublisher.Preparer to use to publish on different topics
  private final LoadingCache<String, KafkaPublisher.Preparer> kafkaPublishersCache;
  private final ConcurrentMap<TopicPartition, KafkaNotificationsCallback> kafkaCallbacks;

  // Executor to publish notifications to Kafka
  private ListeningExecutorService publishingExecutor;

  private KafkaPublisher kafkaPublisher;

  @Inject
  public KafkaNotificationService(KafkaClient kafkaClient, NotificationFeedManager feedManager,
                                  @Named(Constants.Notification.KAFKA_DELEGATE_NOTIFICATION_SERVICE)
                                  InMemoryNotificationService delegate) {
    this.kafkaClient = kafkaClient;
    this.feedManager = feedManager;
    this.delegate = delegate;
    this.ack = KafkaPublisher.Ack.LEADER_RECEIVED;

    // Publisher fields
    this.kafkaPublishersCache =
      CacheBuilder.newBuilder()
        .expireAfterWrite(KAFKA_PUBLISHERS_CACHE_TIMEOUT, TimeUnit.SECONDS)
        .build(new CacheLoader<String, KafkaPublisher.Preparer>() {
          @Override
          public KafkaPublisher.Preparer load(String topic) throws Exception {
            KafkaPublisher publisher = getKafkaPublisher();
            if (kafkaPublisher == null) {
              throw new NotificationFeedException("Unable to get kafka publisher, " +
                                                    "will not be able to publish Notification.");
            }
            return publisher.prepare(topic);
          }
        });

    // Subscriber fields
    this.kafkaCallbacks = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    publishingExecutor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("notification-publisher-%d")));
  }

  @Override
  protected void shutDown() throws Exception {
    publishingExecutor.shutdownNow();
  }

  @Override
  public <N> ListenableFuture<N> publish(NotificationFeed feed, N notification)
    throws NotificationException {
    return publish(feed, notification, notification.getClass());
  }

  @Override
  public <N> ListenableFuture<N> publish(final NotificationFeed feed, final N notification, final Type notificationType)
    throws NotificationException {
    LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);
    return publishingExecutor.submit(new Callable<N>() {
      @Override
      public N call() throws Exception {
        try {
          KafkaMessage message = new KafkaMessage(KafkaNotificationUtils.getMessageKey(feed),
                                                  GSON.toJsonTree(notification, notificationType));
          ByteBuffer bb = KafkaMessageCodec.encode(message);

          TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);
          KafkaPublisher.Preparer preparer = kafkaPublishersCache.get(topicPartition.getTopic());
          preparer.add(bb, message.getMessageKey());

          try {
            preparer.send().get();
            return notification;
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          } catch (ExecutionException e) {
            throw new NotificationException(e.getCause());
          }
        } catch (IOException e) {
          throw new NotificationException(e);
        }
      }
    });
  }

  private KafkaPublisher getKafkaPublisher() {
    if (kafkaPublisher != null) {
      return kafkaPublisher;
    }
    try {
      kafkaPublisher = kafkaClient.getPublisher(ack, Compression.SNAPPY);
    } catch (IllegalStateException e) {
      // can happen if there are no kafka brokers because the kafka server is down.
      kafkaPublisher = null;
    }
    return kafkaPublisher;
  }


  @Override
  public <N> Cancellable subscribe(NotificationFeed feed, NotificationHandler<N> handler)
    throws NotificationFeedException {
    return subscribe(feed, handler, Threads.SAME_THREAD_EXECUTOR);
  }

  @Override
  public <N> Cancellable subscribe(final NotificationFeed feed, final NotificationHandler<N> handler,
                                   Executor executor) throws NotificationFeedException {
    // This call will make sure that the feed exists
    feedManager.getFeed(feed);

    final TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);

    KafkaNotificationsCallback kafkaNotificationsCallback;
    synchronized (kafkaCallbacks) {
      kafkaNotificationsCallback = kafkaCallbacks.get(topicPartition);
      if (kafkaNotificationsCallback == null) {
        kafkaNotificationsCallback = new KafkaNotificationsCallback(topicPartition);
        kafkaCallbacks.putIfAbsent(topicPartition, kafkaNotificationsCallback);
      }
    }
    final KafkaNotificationsCallback kafkaCallback = kafkaNotificationsCallback;
    final Cancellable cancellable = kafkaNotificationsCallback.addSubscription(feed, handler, executor);
    return new Cancellable() {
      @Override
      public void cancel() {
        cancellable.cancel();
        if (kafkaCallback.isEmpty()) {
          kafkaCallback.cancel();
        }
      }
    };
  }

  /**
   * Callback class called when a Kafka message is received. The {@link #onReceived} method will
   * extract the feed ID of the message received, and pass the notification encoded in the message
   * to all handlers that are interested in that feed, using the {@code delegate} in-memory notification
   * service.
   * One callback is created per TopicPartition. It is created by the first subscription to a feed
   * which maps to the TopicPartition.
   */
  private final class KafkaNotificationsCallback implements KafkaConsumer.MessageCallback, Cancellable {

    private final Multimap<NotificationFeed, Cancellable> subscribers;
    private final Cancellable kafkSubscription;
    private final TopicPartition topicPartition;

    private KafkaNotificationsCallback(TopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      this.subscribers = Multimaps.synchronizedMultimap(HashMultimap.<NotificationFeed, Cancellable>create());

      KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

      // TODO there is a bug in twill, that when the topic doesn't exist, add latest will not make subscription
      // start from offset 0 - but that will be fixed soon
      preparer.addLatest(topicPartition.getTopic(), topicPartition.getPartition());
      kafkSubscription = preparer.consume(this);
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
            delegate.publish(KafkaNotificationUtils.getMessageFeed(decodedMessage.getMessageKey()),
                             decodedMessage.getNotificationJson());
          } catch (Throwable t) {
            LOG.warn("Error while processing notification {} with handler {}", decodedMessage.getNotificationJson(), t);
          }
        } catch (IOException e) {
          LOG.error("Could not decode Kafka message {} using Gson.", message, e);
        }
      }
      LOG.debug("Handled {} messages from kafka", count);
    }

    /**
     * Subscribe to a {@code feed} with a {@code handler}, using the delegate {@link InMemoryNotificationService}.
     *
     * @param feed {@link NotificationFeed} to subscribe to
     * @param handler {@link NotificationHandler} to use to process received notifications
     * @param executor {@link Executor} used to push notifications to the handler
     * @param <N> Type of the notifications to handle
     * @return a {@link Cancellable} to cancel the subscription
     * @throws NotificationFeedException if the subscription throught the {@link InMemoryNotificationService} could
     * not be made
     */
    public <N> Cancellable addSubscription(final NotificationFeed feed, NotificationHandler<N> handler,
                                           Executor executor) throws NotificationFeedException {
      final Cancellable cancellable = delegate.subscribe(feed, handler, executor);
      subscribers.put(feed, cancellable);
      return new Cancellable() {
        @Override
        public void cancel() {
          cancellable.cancel();
          subscribers.remove(feed, cancellable);
        }
      };
    }

    @Override
    public void finished() {
      LOG.info("Subscription to topic partition {} finished.", topicPartition);
    }

    @Override
    public void cancel() {
      kafkSubscription.cancel();
      kafkaCallbacks.remove(topicPartition, this);
    }

    /**
     * @return {@code true} if this Kafka message callback is not used to listen to any {@link NotificationFeed}
     * anymore, or {@false} otherwise.
     */
    public boolean isEmpty() {
      return subscribers.isEmpty();
    }
  }
}
