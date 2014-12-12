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

package co.cask.cdap.notifications;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.TxRunnable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.guice.NotificationClientRuntimeModule;
import co.cask.cdap.notifications.service.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationFeedService;
import co.cask.cdap.notifications.service.NotificationFeedStore;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NotificationTest {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationTest.class);

  private static NotificationFeedClient feedClient;
  private static DatasetFramework dsFramework;

  private static TransactionManager txManager;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;

  private static Provider<NotificationPublisher> publisherProvider;
  private static Provider<NotificationSubscriber> subscriberProvider;

  protected NotificationPublisher getNewPublisher() {
    return publisherProvider.get();
  }

  protected NotificationSubscriber getNewSubscriber() {
    return subscriberProvider.get();
  }


  public static Injector createInjector(CConfiguration cConf, Module... modules) {

    return Guice.createInjector(Iterables.concat(
                                  ImmutableList.of(
                                    new ConfigModule(cConf, new Configuration()),
                                    new DiscoveryRuntimeModule().getInMemoryModules(),
                                    new DataSetsModules().getLocalModule(),
                                    new DataSetServiceModules().getInMemoryModule(),
                                    new LocationRuntimeModule().getInMemoryModules(),
                                    new MetricsClientRuntimeModule().getInMemoryModules(),
                                    new ExploreClientModule(),
                                    new IOModule(),
                                    new AuthModule(),
                                    new DataFabricModules().getInMemoryModules(),
                                    new NotificationClientRuntimeModule().getInMemoryModules(),
                                    new AbstractModule() {
                                      @Override
                                      protected void configure() {
                                        bind(NotificationFeedClient.class).to(NotificationFeedClientAndService.class)
                                          .in(Scopes.SINGLETON);
                                        bind(NotificationFeedService.class).to(NotificationFeedClientAndService.class)
                                          .in(Scopes.SINGLETON);
                                        bind(NotificationFeedStore.class).to(InMemoryNotificationFeedStore.class)
                                          .in(Scopes.SINGLETON);
                                      }
                                    }),
                                  Arrays.asList(modules))
    );
  }

  public static void startServices(Injector injector) throws Exception {
    publisherProvider = injector.getProvider(NotificationPublisher.class);
    subscriberProvider = injector.getProvider(NotificationSubscriber.class);

    feedClient = injector.getInstance(NotificationFeedClient.class);
    dsFramework = injector.getInstance(DatasetFramework.class);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
  }

  public static void stopServices() throws Exception {
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
  }


  @Test
  public void onePublisherOneSubscriberTest() throws Exception {
    final int messagesCount = 20;

    NotificationFeed feed = new NotificationFeed.Builder()
      .setNamespace("namespace").setCategory("stream").setName("foo").setDescription("").build();
    Assert.assertTrue(feedClient.createFeed(feed));
    try {
      NotificationPublisher publisher = getNewPublisher();

      final NotificationPublisher.Sender<String> sender = publisher.createSender(feed, String.class);

      try {
        // Runnable to publish notifications on behalf of the publisher entity
        Runnable publisherRunnable = new Runnable() {
          @Override
          public void run() {
            try {
              for (int i = 0; i < messagesCount; i++) {
                sender.send(String.format("fake-payload-%d", i));
                TimeUnit.MILLISECONDS.sleep(100);
              }
            } catch (Throwable e) {
              Throwables.propagate(e);
            }
          }
        };
        Thread publisherThread = new Thread(publisherRunnable);

        // Create a subscribing process
        NotificationSubscriber subscriber = getNewSubscriber();
        NotificationSubscriber.Preparer preparer = subscriber.prepare();

        final AtomicInteger receiveCount = new AtomicInteger(0);
        final AtomicBoolean assertionOk = new AtomicBoolean(true);

        preparer.add(feed, new NotificationHandler<String>() {
          @Override
          public Type getNotificationFeedType() {
            return String.class;
          }

          @Override
          public void processNotification(String notification, NotificationContext notificationContext) {
            LOG.debug("Received notification payload: {}", notification);
            try {
              Assert.assertEquals("fake-payload-" + receiveCount.get(), notification);
              receiveCount.incrementAndGet();
            } catch (Throwable t) {
              assertionOk.set(false);
            }
          }
        });

        Cancellable cancellable = preparer.consume();

        publisherThread.start();
        publisherThread.join();
        TimeUnit.MILLISECONDS.sleep(2000);
        cancellable.cancel();

        Assert.assertTrue(assertionOk.get());
        Assert.assertEquals(messagesCount, receiveCount.get());
      } finally {
        sender.shutdownNow();
        try {
          sender.send("foo");
          Assert.fail();
        } catch (NotificationPublisher.SenderShutdownException e) {
          // Expected.
        }
      }
    } finally {
      feedClient.deleteFeed(feed);
      try {
        feedClient.getFeed(feed);
        Assert.fail("Should throw NotificationFeedNotFoundException.");
      } catch (NotificationFeedNotFoundException e) {
        // Expected
      }
    }
  }

  @Test
  public void feedNotCreatedTest() throws Exception {
    NotificationFeed feed = new NotificationFeed.Builder()
      .setNamespace("namespace").setCategory("stream").setName("foo").setDescription("").build();

    try {
      // Try subscribing to a feed before creating it
      NotificationSubscriber subscriber = getNewSubscriber();
      NotificationSubscriber.Preparer preparer = subscriber.prepare();
      preparer.add(feed, new NotificationHandler<String>() {
        @Override
        public Type getNotificationFeedType() {
          return String.class;
        }

        @Override
        public void processNotification(String notification, NotificationContext notificationContext) {
          // No-op
        }
      });
      Assert.fail("Should throw NotificationFeedNotFoundException.");
    } catch (NotificationFeedNotFoundException e) {
      // Expected
    }

    try {
      // Try publishing to a feed before creating it
      NotificationPublisher publisher = getNewPublisher();
      publisher.createSender(feed, String.class);
      Assert.fail("Should throw NotificationFeedNotFoundException.");
    } catch (NotificationFeedNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void useTransactionTest() throws Exception {
    // Performing admin operations to create dataset instance
    // keyValueTable is a system dataset module
    dsFramework.addInstance("keyValueTable", "myTable", DatasetProperties.EMPTY);

    NotificationFeed feed = new NotificationFeed.Builder()
      .setNamespace("namespace").setCategory("stream").setName("foo").setDescription("").build();

    Assert.assertTrue(feedClient.createFeed(feed));
    try {
      NotificationSubscriber subscriber = getNewSubscriber();
      NotificationSubscriber.Preparer preparer = subscriber.prepare();
      preparer.add(feed, new NotificationHandler<String>() {
        @Override
        public Type getNotificationFeedType() {
          return String.class;
        }

        @Override
        public void processNotification(final String notification, NotificationContext notificationContext) {
          notificationContext.execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              KeyValueTable table = context.getDataset("myTable");
              table.write("foo", notification);
            }
          }, TxRetryPolicy.dropAfter(5));
        }
      });
      Cancellable cancellable = preparer.consume();

      NotificationPublisher.Sender<String> sender = getNewPublisher().createSender(feed, String.class);
      try {
        sender.send("foobar");
        // Waiting for the subscriber to receive that notification
        TimeUnit.SECONDS.sleep(5);

        KeyValueTable table = dsFramework.getDataset("myTable", DatasetDefinition.NO_ARGUMENTS, null);
        Assert.assertNotNull(table);
        Transaction tx1 = txManager.startShort(100);
        table.startTx(tx1);
        Assert.assertEquals("foobar", Bytes.toString(table.read("foo")));
        Assert.assertTrue(table.commitTx());
        txManager.canCommit(tx1, table.getTxChanges());
        txManager.commit(tx1);
        table.postTxCommit();
      } finally {
        sender.shutdownNow();
        cancellable.cancel();
      }
    } finally {
      dsFramework.deleteInstance("myTable");
      feedClient.deleteFeed(feed);
    }
  }

  // TODO other tests: one publisher multiple subscribers
  // mutliple publishers one subscriber for all feeds, and for only one of the feeds (making sure not
  // everybody receives everything)

//  @Test
//  public void onePublisherMultipleSubscribers() throws Exception {
//    final int messagesCount = 5;
//    int subscribersCount = 1;
//    final Publisher publisher = new Publisher(ActorType.QUERY, "fake-query-handle-2");
//    Runnable publisherRunnable = new Runnable() {
//      @Override
//      public void run() {
//        try {
//          for (int i = 0; i < messagesCount; i++) {
//            getNotificationPublisher().publish(publisher,
//                                               new Notification(0, String.format("fake-payload-%d", i).getBytes()));
//            TimeUnit.MILLISECONDS.sleep(100);
//          }
//        } catch (Throwable e) {
//          Throwables.propagate(e);
//        }
//      }
//    };
//    Thread publisherThread = new Thread(publisherRunnable);
//    publisherThread.start();
//
//    final int[] receiveCounts = new int[subscribersCount];
//    final AtomicBoolean assertionOk = new AtomicBoolean(true);
//    List<NotificationSubscriber> notificationSubscribers = Lists.newArrayList();
//    for (int i = 0; i < subscribersCount; i++) {
//      final int j = i;
//      final Subscriber subscriber = new Subscriber(ActorType.DATASET, "fake-dataset-" + i);
//      NotificationSubscriber notificationSubscriber = getNotificationSubscriber(subscriber);
//      notificationSubscribers.add(notificationSubscriber);
//
//      notificationSubscriber.subscribe(publisher, new NotificationHandler() {
//        @Override
//        public void handle(Notification notification) {
//          String payloadStr = Bytes.toString(notification.getPayload());
//          LOG.debug("Received notification for subscriberID {}, payload: {}", subscriber, payloadStr);
//          try {
//            Assert.assertEquals(String.format("fake-payload-%d", receiveCounts[j]), payloadStr);
//            receiveCounts[j]++;
//          } catch (Throwable t) {
//            assertionOk.set(false);
//          }
//        }
//      });
//    }
//
//    publisherThread.join();
//    TimeUnit.MILLISECONDS.sleep(2000);
//    for (NotificationSubscriber notificationSubscriber : notificationSubscribers) {
//      Closeables.closeQuietly(notificationSubscriber);
//    }
//
//    Assert.assertTrue(assertionOk.get());
//    for (int i : receiveCounts) {
//      Assert.assertEquals(messagesCount, i);
//    }
//  }
}
