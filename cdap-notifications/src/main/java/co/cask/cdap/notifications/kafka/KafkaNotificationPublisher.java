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

import co.cask.cdap.notifications.NotificationFeed;
import co.cask.cdap.notifications.NotificationPublisher;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.service.NotificationFeedException;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Kafka implementation of a {@link co.cask.cdap.notifications.NotificationSubscriber}.
 */
// TODO worry about thread safety
public class KafkaNotificationPublisher implements NotificationPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationPublisher.class);

  private final KafkaClient kafkaClient;
  private final KafkaPublisher.Ack ack;

  private KafkaPublisher kafkaPublisher;
  private final NotificationFeedClient notificationFeedClient;

  @Inject
  public KafkaNotificationPublisher(KafkaClient kafkaClient, NotificationFeedClient notificationFeedClient) {
    this.kafkaClient = kafkaClient;
    this.notificationFeedClient = notificationFeedClient;

    // TODO think about that ack
    this.ack = KafkaPublisher.Ack.LEADER_RECEIVED;
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
  public <N> Sender<N> createSender(final NotificationFeed feed, Class<N> notificationType)
    throws NotificationFeedException {
    final KafkaPublisher kafkaPublisher = getKafkaPublisher();
    if (kafkaPublisher == null) {
      throw new NotificationFeedException("Unable to get kafka publisher, will not be able to publish Notification.");
    }

    // This call will make sure that the feed exists - at this point it really should
    // Because only the resource owner can publish changes from the resource
    // And the resource owner should have created the feed first hand.
    notificationFeedClient.getFeed(feed);

    return new Sender<N>() {

      private final ExecutorService executor = Executors.newSingleThreadExecutor(
        Threads.createDaemonThreadFactory("notification-sender-%d"));

      @Override
      public void shutdown() {
        executor.shutdown();
      }

      @Override
      public boolean isShutdown() {
        return executor.isShutdown();
      }

      @Override
      public List<N> shutdownNow() {
        List<Runnable> runnables = executor.shutdownNow();
        ImmutableList.Builder<N> builder = ImmutableList.builder();
        for (Runnable r : runnables) {
          if (r instanceof GetNotificationRunnable) {
            builder.add(((GetNotificationRunnable<N>) r).getNotification());
          }
        }
        return builder.build();
      }

      @Override
      public ListenableFuture<Void> send(N notification) throws IOException, SenderShutdownException {
        if (isShutdown()) {
          LOG.warn("Cannot publish notification, sender is shut down.");
          throw new SenderShutdownException("Tried to publish notification " + notification + " after shut down.");
        }

        LOG.debug("Publishing on notification feed [{}]: {}", feed, notification);

        ByteBuffer bb = ByteBuffer.wrap(KafkaMessageSerializer.encode(feed, notification));
        String topic = KafkaNotificationUtils.getKafkaTopic(feed);
        KafkaPublisher.Preparer preparer = kafkaPublisher.prepare(topic);
        preparer.add(bb, KafkaMessageSerializer.buildKafkaMessageKey(feed));

        final ListenableFuture<Integer> future = preparer.send();
        executor.submit(new GetNotificationRunnable<N>(future, notification));
        return Futures.transform(future, Functions.<Void>constant(null));
      }
    };
  }

  /**
   * Runnable which only job is to wait for the get method of a future representing the pushing of Notification
   * to return.
   *
   * @param <N> Type of the Notification being pushed.
   */
  private static final class GetNotificationRunnable<N> implements Runnable {

    private final ListenableFuture<Integer> future;
    private final N notification;

    public GetNotificationRunnable(ListenableFuture<Integer> future, N notification) {
      this.future = future;
      this.notification = notification;
    }

    public N getNotification() {
      return notification;
    }

    @Override
    public void run() {
      try {
        future.get();
      } catch (Exception e) {
        LOG.error("Could not get publish notification result", e);
        Throwables.propagate(e);
      }
    }
  }
}
