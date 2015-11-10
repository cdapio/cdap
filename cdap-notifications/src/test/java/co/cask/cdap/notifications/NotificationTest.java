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

package co.cask.cdap.notifications;

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.notifications.service.TxRetryPolicy;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class NotificationTest {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationTest.class);

  protected static NotificationFeedManager feedManager;
  private static DatasetFramework dsFramework;
  private static TransactionSystemClient txClient;
  private static TransactionManager txManager;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static AbstractNamespaceClient namespaceClient;
  private static NotificationService notificationService;

  private static final Id.Namespace namespace = Id.Namespace.from("namespace");
  protected static final Id.NotificationFeed FEED1 = new Id.NotificationFeed.Builder()
    .setNamespaceId(namespace.getId()).setCategory("stream").setName("foo").setDescription("").build();
  protected static final Id.NotificationFeed FEED2 = new Id.NotificationFeed.Builder()
    .setNamespaceId(namespace.getId()).setCategory("stream").setName("bar").setDescription("").build();

  protected static NotificationService getNotificationService() {
    return notificationService;
  }

  public static Injector createInjector(CConfiguration cConf, Module... modules) {

    return Guice.createInjector(Iterables.concat(
                                  ImmutableList.of(
                                    new ConfigModule(cConf, new Configuration()),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new DataSetsModules().getStandaloneModules(),
                                    new DataSetServiceModules().getInMemoryModules(),
                                    new LocationRuntimeModule().getInMemoryModules(),
                                    new MetricsClientRuntimeModule().getInMemoryModules(),
                                    new ExploreClientModule(),
                                    new IOModule(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new NamespaceClientRuntimeModule().getInMemoryModules()
                                  ),
                                  Arrays.asList(modules))
    );
  }

  public static void startServices(Injector injector) throws Exception {
    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();

    feedManager = injector.getInstance(NotificationFeedManager.class);
    dsFramework = injector.getInstance(DatasetFramework.class);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    txClient = injector.getInstance(TransactionSystemClient.class);
    namespaceClient = injector.getInstance(AbstractNamespaceClient.class);
  }

  public static void stopServices() throws Exception {
    notificationService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
  }

  @Test (expected = NotificationFeedNotFoundException.class)
  public void feedNotCreatedTest() throws Exception {
    notificationService.subscribe(FEED1, new NotificationHandler<String>() {
      @Override
      public Type getNotificationType() {
        return String.class;
      }

      @Override
      public void received(String notification, NotificationContext notificationContext) {
        // No-op
      }
    });
  }

  @Test
  public void testCreateGetAndListFeeds() throws Exception {
    // no feeds at the beginning
    Assert.assertEquals(0, feedManager.listFeeds(namespace).size());
    // create feed 1
    feedManager.createFeed(FEED1);
    // check get and list feed
    Assert.assertEquals(FEED1, feedManager.getFeed(FEED1));
    Assert.assertEquals(ImmutableList.of(FEED1), feedManager.listFeeds(namespace));

    // create feed 2
    feedManager.createFeed(FEED2);
    // check get and list feed
    Assert.assertEquals(FEED2, feedManager.getFeed(FEED2));
    Assert.assertTrue(checkFeedList(ImmutableSet.of(FEED1, FEED2), feedManager.listFeeds(namespace)));

    // clear the feeds
    feedManager.deleteFeed(FEED1);
    feedManager.deleteFeed(FEED2);
    namespaceClient.delete(namespace);
    Assert.assertEquals(0, feedManager.listFeeds(namespace).size());
  }

  private boolean checkFeedList(ImmutableSet<Id.NotificationFeed> expected, List<Id.NotificationFeed> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (Id.NotificationFeed feed : actual) {
      if (!expected.contains(feed)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void useTransactionTest() throws Exception {
    // Performing admin operations to create dataset instance
    // keyValueTable is a system dataset module
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespace).build());
    Id.DatasetInstance myTableInstance = Id.DatasetInstance.from(namespace, "myTable");
    dsFramework.addInstance("keyValueTable", myTableInstance, DatasetProperties.EMPTY);

    final CountDownLatch receivedLatch = new CountDownLatch(1);
    Assert.assertTrue(feedManager.createFeed(FEED1));
    try {
      Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<String>() {
        private int received = 0;

        @Override
        public Type getNotificationType() {
          return String.class;
        }

        @Override
        public void received(final String notification, NotificationContext notificationContext) {
          notificationContext.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              KeyValueTable table = context.getDataset("myTable");
              table.write("foo", String.format("%s-%d", notification, received++));
              receivedLatch.countDown();
            }
          }, TxRetryPolicy.maxRetries(5));
        }
      });
      // Short delay for the subscriber to setup the subscription.
      TimeUnit.MILLISECONDS.sleep(500);

      try {
        notificationService.publish(FEED1, "foobar");
        // Waiting for the subscriber to receive that notification
        Assert.assertTrue(receivedLatch.await(5, TimeUnit.SECONDS));

        // Read the KeyValueTable for the value updated from the subscriber.
        // Need to poll it couple times since after the received method returned,
        // the tx may not yet committed when we try to read it here.
        final KeyValueTable table = dsFramework.getDataset(myTableInstance, DatasetDefinition.NO_ARGUMENTS, null);
        Assert.assertNotNull(table);

        final TransactionContext txContext = new TransactionContext(txClient, table);
        Tasks.waitFor(true, new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            txContext.start();
            try {
              return "foobar-0".equals(Bytes.toString(table.read("foo")));
            } finally {
              txContext.finish();
            }
          }
        }, 5, TimeUnit.SECONDS);
      } finally {
        cancellable.cancel();
      }
    } finally {
      dsFramework.deleteInstance(myTableInstance);
      feedManager.deleteFeed(FEED1);
      namespaceClient.delete(namespace);
    }
  }

  @Test
  public void onePublisherOneSubscriberTest() throws Exception {
    Set<Id.NotificationFeed> feeds = ImmutableSet.of(FEED1);
    testPubSub(feeds, 1, 20, feeds, 1, String.class, new Function<SimpleNotification, String>() {
      @Override
      public String apply(SimpleNotification input) {
        return input.getPayload();
      }
    });
  }

  @Test
  public void onePublisherMultipleSubscribersTest() throws Exception {
    Set<Id.NotificationFeed> feeds = ImmutableSet.of(FEED1);
    testPubSub(feeds, 1, 20, feeds, 10, String.class, new Function<SimpleNotification, String>() {
      @Override
      public String apply(SimpleNotification input) {
        return input.getPayload();
      }
    });
  }

  @Test
  public void multiplePublishersOneSubscriberTest() throws Exception {
    /*
      This configuration should not happen, as, by design, we want only one publisher to publisher the changes attached
      to a resource. But since the low level APIs allow it, this should still be tested.
     */
    Set<Id.NotificationFeed> feeds = ImmutableSet.of(FEED1);
    testPubSub(feeds, 5, 15, feeds, 1, String.class, new Function<SimpleNotification, String>() {
      @Override
      public String apply(SimpleNotification input) {
        return input.getPayload();
      }
    });
  }

  @Test
  public void multipleFeedsOneSubscriber() throws Exception {
    Set<Id.NotificationFeed> feeds = ImmutableSet.of(FEED1, FEED2);
    testPubSub(feeds, 1, 15, feeds, 1, SimpleNotification.class, Functions.<SimpleNotification>identity());
  }

  @Test
  public void twoFeedsPublishOneFeedSubscribeTest() throws Exception {
    // Test two publishers on two different feeds, but only one subscriber subscribing to one of the feeds
    testPubSub(ImmutableSet.of(FEED1, FEED2), 1, 15,
               ImmutableSet.of(FEED1), 1, SimpleNotification.class, Functions.<SimpleNotification>identity());
  }

  /**
   * Testing publishers/subscribers interaction.
   *
   * @param pubFeeds set of feeds to publish to
   * @param publishersPerFeed number of publishers doing concurrent publishing for each feed
   * @param messagesPerPublisher number of messages being published by each publisher
   * @param subFeeds set of feeds to subscribe to
   * @param subscribersPerFeed number of subscribers for each feed
   * @param payloadType Class reprenseting the data type of the payload of the notification
   * @param payloadFunction a function that transform {@link SimpleNotification} type to the payload type
   * @param <T> type of the payload
   */
  private <T> void testPubSub(Set<Id.NotificationFeed> pubFeeds, int publishersPerFeed, final int messagesPerPublisher,
                              Set<Id.NotificationFeed> subFeeds, int subscribersPerFeed,
                              final Class<T> payloadType,
                              final Function<SimpleNotification, T> payloadFunction) throws Exception {

    for (Id.NotificationFeed feedId : Sets.union(pubFeeds, subFeeds)) {
      Assert.assertTrue(feedManager.createFeed(feedId));
    }
    try {
      int totalMessages = subFeeds.size() * publishersPerFeed * messagesPerPublisher * subscribersPerFeed;
      final CountDownLatch latch = new CountDownLatch(totalMessages);
      final Queue<T> receivedMessages = new ConcurrentLinkedQueue<>();
      List<Cancellable> cancellables = Lists.newArrayList();

      try {
        for (Id.NotificationFeed feedId : subFeeds) {
          for (int i = 0; i < subscribersPerFeed; i++) {
            Cancellable cancellable = notificationService.subscribe(feedId, new NotificationHandler<T>() {

              @Override
              public Type getNotificationType() {
                return payloadType;
              }

              @Override
              public void received(T notification, NotificationContext notificationContext) {
                LOG.debug("Received notification payload: {}", notification);
                receivedMessages.offer(notification);
                latch.countDown();
              }
            });
            cancellables.add(cancellable);
          }
        }

        // Give the subscriber some time to prepare for published messages before starting the publisher
        TimeUnit.MILLISECONDS.sleep(500);

        // Starts publishers
        final Map<Id.NotificationFeed, Queue<T>> publishedMessages = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(pubFeeds.size() * publishersPerFeed);
        try {
          for (final Id.NotificationFeed feedId : pubFeeds) {
            final Queue<T> publishedQueue = new ConcurrentLinkedQueue<>();
            publishedMessages.put(feedId, publishedQueue);

            // Let all publishers start together
            final CyclicBarrier publisherBarrier = new CyclicBarrier(publishersPerFeed);
            for (int i = 0; i < publishersPerFeed; i++) {
              final int publisherId = i;
              executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  publisherBarrier.await();
                  for (int i = 0; i < messagesPerPublisher; i++) {
                    T notification = payloadFunction.apply(new SimpleNotification(publisherId,
                                                                                  String.format("%s-%d", feedId, i)));
                    notificationService.publish(feedId, notification);
                    publishedQueue.add(notification);
                    TimeUnit.MILLISECONDS.sleep(10);
                  }
                  return null;
                }
              });
            }
          }

          // Wait for subscriptions getting all messages
          Assert.assertTrue(latch.await(5000, TimeUnit.SECONDS));
        } finally {
          executor.shutdown();
        }

        // Verify the result.
        Multiset<T> received = HashMultiset.create(receivedMessages);
        Assert.assertEquals(totalMessages, received.size());

        // For each unique message published that has subscription,
        // there should be (publisher per feed * subscriber per feed) of them
        for (Id.NotificationFeed feedId : subFeeds) {
          for (T notification : ImmutableMultiset.copyOf(publishedMessages.get(feedId)).elementSet()) {
            Assert.assertEquals(publishersPerFeed * subscribersPerFeed, received.count(notification));
          }
        }
      } finally {
        for (Cancellable cancellable : cancellables) {
          cancellable.cancel();
        }
      }
    } finally {
      for (Id.NotificationFeed feedId : Sets.union(pubFeeds, subFeeds)) {
        feedManager.deleteFeed(feedId);
      }
    }
  }

  private static final class SimpleNotification {
    private final int publisherId;
    private final String payload;

    private SimpleNotification(int publisherId, String payload) {
      this.publisherId = publisherId;
      this.payload = payload;
    }

    public String getPayload() {
      return payload;
    }

    @Override
    public String toString() {
      return String.format("id: %d, payload: %s", publisherId, payload);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleNotification that = (SimpleNotification) o;
      return Objects.equals(publisherId, that.publisherId) &&
        Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
      return Objects.hash(publisherId, payload);
    }
  }
}
