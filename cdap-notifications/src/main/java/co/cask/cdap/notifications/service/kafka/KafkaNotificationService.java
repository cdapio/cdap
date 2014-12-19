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

package co.cask.cdap.notifications.service.kafka;

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.BasicNotificationContext;
import co.cask.cdap.notifications.service.NotificationException;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Kafka implementation of the {@link NotificationService}.
 */
public class KafkaNotificationService extends AbstractIdleService implements NotificationService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationService.class);
  private static final Gson GSON = new Gson();

  private static final int KAFKA_PUBLISHERS_CACHE_TIMEOUT = 3600;

  private final KafkaClient kafkaClient;
  private final DatasetFramework dsFramework;
  private final NotificationFeedManager feedManager;
  private final TransactionSystemClient transactionSystemClient;
  private final KafkaPublisher.Ack ack;

  private KafkaPublisher kafkaPublisher;
  private final LoadingCache<String, KafkaPublisher.Preparer> kafkaPublishersCache;
  private ListeningExecutorService publishingExecutor;

  private final Map<TopicPartition, Cancellable> kafkaConsumers;
  private final Lock consumersLock;
  private final SetMultimap<TopicPartition, NotificationFeed> topicsToFeeds;
  private final ListMultimap<NotificationFeed, NotificationHandler> feedsToHandlers;
  private final ReadWriteLock topicsFeedsLock;

  @Inject
  public KafkaNotificationService(KafkaClient kafkaClient, DatasetFramework dsFramework,
                                  NotificationFeedManager feedManager,
                                  TransactionSystemClient transactionSystemClient) {
    this.kafkaClient = kafkaClient;
    this.dsFramework = dsFramework;
    this.feedManager = feedManager;
    this.transactionSystemClient = transactionSystemClient;
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
    this.kafkaConsumers = Maps.newHashMap();
    this.consumersLock = new ReentrantLock();
    this.topicsToFeeds = HashMultimap.create();
    this.feedsToHandlers = LinkedListMultimap.create();
    this.topicsFeedsLock = new ReentrantReadWriteLock();
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
    throws NotificationException, NotificationFeedException {
    return publish(feed, notification, notification.getClass());
  }

  @Override
  public <N> ListenableFuture<N> publish(final NotificationFeed feed, final N notification, final Type notificationType)
    throws NotificationException, NotificationFeedException {
    try {
      TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);
      final KafkaPublisher.Preparer preparer = kafkaPublishersCache.get(topicPartition.getTopic());
      LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);
      return publishingExecutor.submit(new Callable<N>() {
        @Override
        public N call() throws Exception {
          try {
            KafkaMessage message = new KafkaMessage(KafkaNotificationUtils.buildKafkaMessageKey(feed),
                                                    GSON.toJsonTree(notification, notificationType));
            ByteBuffer bb = KafkaMessageIO.encode(message);
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
    } catch (ExecutionException e) {
      throw new NotificationException(Throwables.getRootCause(e));
    }
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
    return subscribe(feed, handler, null);
  }

  @Override
  public <N> Cancellable subscribe(final NotificationFeed feed, final NotificationHandler<N> handler,
                                   Executor executor) throws NotificationFeedException {
    // This call will make sure that the feed exists
    feedManager.getFeed(feed);

    final TopicPartition topicPartition = KafkaNotificationUtils.getKafkaTopicPartition(feed);

    topicsFeedsLock.writeLock().lock();
    try {
      // Because topicsToFeeds is a SetMultimap, this call will have no effect if
      // the feed is already mapped to that partition
      topicsToFeeds.put(topicPartition, feed);
      feedsToHandlers.put(feed, handler);
    } finally {
      topicsFeedsLock.writeLock().unlock();
    }

    consumersLock.lock();
    try {
      if (kafkaConsumers.get(topicPartition) == null) {
        KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();

        // TODO there is a bug in twill, that when the topic doesn't exist, add latest will not make subscription
        // start from offset 0 - but that will be fixed soon
        preparer.addLatest(topicPartition.getTopic(), topicPartition.getPartition());

        Cancellable cancellableSubscriber = preparer.consume(new KafkaConsumer.MessageCallback() {
          @Override
          public void onReceived(Iterator<FetchedMessage> messages) {
            int count = 0;
            while (messages.hasNext()) {
              FetchedMessage message = messages.next();
              ByteBuffer payload = message.getPayload();

              topicsFeedsLock.readLock().lock();
              try {
                KafkaMessage decodedMessage = KafkaMessageIO.decode(payload);
                for (NotificationFeed feedForTopic : topicsToFeeds.get(topicPartition)) {
                  if (!decodedMessage.getMessageKey().equals(
                    KafkaNotificationUtils.buildKafkaMessageKey(feedForTopic))) {
                    continue;
                  }

                  for (NotificationHandler handlerForFeed : feedsToHandlers.get(feedForTopic)) {
                    try {
                      // All handlers of one feed must handle notifications of the same type, otherwise some handlers
                      // will fail to decode the notification
                      Object notification = GSON.fromJson(decodedMessage.getNotificationJson(),
                                                          handlerForFeed.getNotificationFeedType());
                      try {
                        handlerForFeed.processNotification(notification,
                                                           new BasicNotificationContext(dsFramework,
                                                                                        transactionSystemClient));
                        count++;
                      } catch (Throwable t) {
                        LOG.warn("Error while processing notification {} with handler {}",
                                 notification, handlerForFeed, t);
                      }
                    } catch (JsonSyntaxException e) {
                      LOG.info("Could not decode Kafka message '{}' using Gson for handler {}. " +
                                 "Make sure that the getNotificationFeedType() method is correctly set.",
                               decodedMessage, handlerForFeed);
                    }
                  }
                }
              } catch (IOException e) {
                LOG.error("Could not decode Kafka message {} using Gson.", message, e);
              } finally {
                topicsFeedsLock.readLock().unlock();
              }
            }
            LOG.debug("Successfully handled {} messages from kafka", count);
          }

          @Override
          public void finished() {
            LOG.info("Subscription to feed {} for handler {} finished.", feed, handler);
          }
        });
        kafkaConsumers.put(topicPartition, cancellableSubscriber);
      }
    } finally {
      consumersLock.unlock();
    }

    return new Cancellable() {
      @Override
      public void cancel() {
        topicsFeedsLock.writeLock().lock();
        try {
          feedsToHandlers.remove(feed, handler);
          // Stop listening to the feed only if there are no more handlers for that feed
          if (feedsToHandlers.get(feed).isEmpty()) {
            topicsToFeeds.remove(topicPartition, feed);
            if (topicsToFeeds.get(topicPartition).isEmpty()) {
              consumersLock.lock();
              try {
                // We now have a Kafka subscriber for a topic that is not listening to any feed anymore
                kafkaConsumers.remove(topicPartition).cancel();
              } finally {
                consumersLock.unlock();
              }
            }
          }
        } finally {
          topicsFeedsLock.writeLock().unlock();
        }
      }
    };
  }
}
