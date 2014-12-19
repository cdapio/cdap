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
import co.cask.cdap.notifications.client.NotificationService;
import co.cask.cdap.notifications.service.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationFeedService;
import co.cask.cdap.notifications.service.NotificationFeedStore;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class NotificationTest {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationTest.class);

  protected static NotificationFeedClient feedClient;
  private static DatasetFramework dsFramework;

  private static TransactionManager txManager;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;

  private static NotificationService notificationService;

  protected static final NotificationFeed FEED1 = new NotificationFeed.Builder()
    .setNamespace("namespace").setCategory("stream").setName("foo").setDescription("").build();
  protected static final NotificationFeed FEED2 = new NotificationFeed.Builder()
    .setNamespace("namespace").setCategory("stream").setName("bar").setDescription("").build();

  protected static NotificationService getNotificationService() {
    return notificationService;
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
    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();

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

    Assert.assertTrue(feedClient.createFeed(FEED1));
    try {
      // Create a subscribing process
      final AtomicInteger receiveCount = new AtomicInteger(0);
      final AtomicBoolean assertionOk = new AtomicBoolean(true);

      Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<String>() {
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
            Throwables.propagate(t);
          }
        }
      });

      // Runnable to publish notifications on behalf of the publisher entity
      Runnable publisherRunnable = new Runnable() {
        @Override
        public void run() {
          try {
            for (int i = 0; i < messagesCount; i++) {
              notificationService.publish(FEED1, String.format("fake-payload-%d", i));
              TimeUnit.MILLISECONDS.sleep(100);
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread = new Thread(publisherRunnable);

      // Give the subscriber some time to prepare for published messages before starting the publisher
      TimeUnit.MILLISECONDS.sleep(500);
      publisherThread.start();
      publisherThread.join();
      TimeUnit.MILLISECONDS.sleep(2000);
      cancellable.cancel();

      Assert.assertTrue(assertionOk.get());
      Assert.assertEquals(messagesCount, receiveCount.get());
    } finally {
      feedClient.deleteFeed(FEED1);
      try {
        feedClient.getFeed(FEED1);
        Assert.fail("Should throw NotificationFeedNotFoundException.");
      } catch (NotificationFeedNotFoundException e) {
        // Expected
      }
    }
  }

  @Test
  public void feedNotCreatedTest() throws Exception {
    try {
      // Try subscribing to a feed before creating it
      notificationService.subscribe(FEED1, new NotificationHandler<String>() {
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
  }

  @Test
  public void useTransactionTest() throws Exception {
    // Performing admin operations to create dataset instance
    // keyValueTable is a system dataset module
    dsFramework.addInstance("keyValueTable", "myTable", DatasetProperties.EMPTY);

    Assert.assertTrue(feedClient.createFeed(FEED1));
    try {
      Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<String>() {
        private int received = 0;

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
              table.write("foo", String.format("%s-%d", notification, received++));
            }
          }, TxRetryPolicy.maxRetries(5));
        }
      });
      TimeUnit.SECONDS.sleep(2);

      try {
        notificationService.publish(FEED1, "foobar");
        // Waiting for the subscriber to receive that notification
        TimeUnit.SECONDS.sleep(2);

        KeyValueTable table = dsFramework.getDataset("myTable", DatasetDefinition.NO_ARGUMENTS, null);
        Assert.assertNotNull(table);
        Transaction tx1 = txManager.startShort(100);
        table.startTx(tx1);
        Assert.assertEquals("foobar-0", Bytes.toString(table.read("foo")));
        Assert.assertTrue(table.commitTx());
        txManager.canCommit(tx1, table.getTxChanges());
        txManager.commit(tx1);
        table.postTxCommit();
      } finally {
        cancellable.cancel();
      }
    } finally {
      dsFramework.deleteInstance("myTable");
      feedClient.deleteFeed(FEED1);
    }
  }

  @Test
  public void onePublisherMultipleSubscribersTest() throws Exception {
    final int messagesCount = 20;
    int subscribersCount = 10;

    Assert.assertTrue(feedClient.createFeed(FEED1));
    try {
      Runnable publisherRunnable = new Runnable() {
        @Override
        public void run() {
          try {
            for (int i = 0; i < messagesCount; i++) {
              notificationService.publish(FEED1, String.format("fake-payload-%d", i));
              TimeUnit.MILLISECONDS.sleep(100);
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread = new Thread(publisherRunnable);

      final int[] receiveCounts = new int[subscribersCount];
      final AtomicBoolean assertionOk = new AtomicBoolean(true);
      List<Cancellable> cancellables = Lists.newArrayList();
      for (int i = 0; i < subscribersCount; i++) {
        final int j = i;

        Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<String>() {
          @Override
          public Type getNotificationFeedType() {
            return String.class;
          }

          @Override
          public void processNotification(String notification, NotificationContext notificationContext) {
            LOG.debug("Received notification payload: {}", notification);
            try {
              Assert.assertEquals("fake-payload-" + receiveCounts[j], notification);
              receiveCounts[j]++;
            } catch (Throwable t) {
              assertionOk.set(false);
              Throwables.propagate(t);
            }
          }
        });
        cancellables.add(cancellable);
      }

      // Give the subscriber some time to prepare for published messages before starting the publisher
      TimeUnit.MILLISECONDS.sleep(500);
      publisherThread.start();
      publisherThread.join();
      TimeUnit.MILLISECONDS.sleep(2000);
      for (Cancellable cancellable : cancellables) {
        cancellable.cancel();
      }

      Assert.assertTrue(assertionOk.get());
      for (int i : receiveCounts) {
        Assert.assertEquals(messagesCount, i);
      }
    } finally {
      feedClient.deleteFeed(FEED1);
    }
  }

  @Test
  public void multiplePublishersOneSubscriberTest() throws Exception {
    /*
      This configuration should not happen, as, by design, we want only one publisher to publisher the changes attached
      to a resource. But since the low level APIs allow it, this should still be tested.
     */

    final int messagesCount = 15;
    int publishersCount = 5;

    Assert.assertTrue(feedClient.createFeed(FEED1));
    try {
      // Create a subscribing process
      final AtomicBoolean assertionOk = new AtomicBoolean(true);
      final int[] receiveCounts = new int[publishersCount];

      Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<SimpleNotification>() {
        @Override
        public Type getNotificationFeedType() {
          return SimpleNotification.class;
        }

        @Override
        public void processNotification(SimpleNotification notification, NotificationContext notificationContext) {
          LOG.debug("Received notification payload: {}", notification);
          try {
            Assert.assertEquals("fake-payload-" + receiveCounts[notification.getPublisherId()], notification.getPayload());
            receiveCounts[notification.getPublisherId()]++;
          } catch (Throwable t) {
            assertionOk.set(false);
            Throwables.propagate(t);
          }
        }
      });

      // Give the subscriber some time to prepare for published messages before starting the publisher
      TimeUnit.MILLISECONDS.sleep(500);

      List<Thread> publisherThreads = Lists.newArrayList();
      for (int i = 0; i < publishersCount; i++) {
        final int k = i;
        Runnable publisherRunnable = new Runnable() {
          @Override
          public void run() {
            try {
              Random r = new Random();
              for (int j = 0; j < messagesCount; j++) {
                notificationService.publish(FEED1, new SimpleNotification(k, String.format("fake-payload-%d", j)));
                TimeUnit.MILLISECONDS.sleep(r.nextInt(200));
              }
            } catch (Throwable e) {
              Throwables.propagate(e);
            }
          }
        };
        Thread publisherThread = new Thread(publisherRunnable);
        publisherThread.start();
        publisherThreads.add(publisherThread);
      }

      for (Thread t : publisherThreads) {
        t.join();
      }
      TimeUnit.MILLISECONDS.sleep(2000);
      cancellable.cancel();

      Assert.assertTrue(assertionOk.get());
      for (int i : receiveCounts) {
        Assert.assertEquals(messagesCount, i);
      }
    } finally {
      feedClient.deleteFeed(FEED1);
    }
  }

  @Test
  public void multipleFeedsOneSubscriber() throws Exception {
    // One subscriber subscribes to two feeds
    final int messagesCount = 15;

    Assert.assertTrue(feedClient.createFeed(FEED1));
    Assert.assertTrue(feedClient.createFeed(FEED2));
    try {
      // Create a subscribing process
      final AtomicBoolean assertionOk = new AtomicBoolean(true);
      final int[] receiveCounts = new int[2];

      List<Cancellable> cancellables = Lists.newArrayList();
      cancellables.add(notificationService.subscribe(FEED1, new NotificationHandler<SimpleNotification>() {
        @Override
        public Type getNotificationFeedType() {
          return SimpleNotification.class;
        }

        @Override
        public void processNotification(SimpleNotification notification, NotificationContext notificationContext) {
          LOG.debug("Received notification payload: {}", notification);
          try {
            Assert.assertEquals("fake-payload-" + receiveCounts[0], notification.getPayload());
            receiveCounts[0]++;
          } catch (Throwable t) {
            assertionOk.set(false);
            Throwables.propagate(t);
          }
        }
      }));

      cancellables.add(notificationService.subscribe(FEED2, new NotificationHandler<String>() {
        @Override
        public Type getNotificationFeedType() {
          return String.class;
        }

        @Override
        public void processNotification(String notification, NotificationContext notificationContext) {
          LOG.debug("Received notification payload: {}", notification);
          try {
            Assert.assertEquals("fake-payload-" + receiveCounts[1], notification);
            receiveCounts[1]++;
          } catch (Throwable t) {
            assertionOk.set(false);
            Throwables.propagate(t);
          }
        }
      }));

      // Give the subscriber some time to prepare for published messages before starting the publisher
      TimeUnit.MILLISECONDS.sleep(500);

      Runnable publisherRunnable1 = new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            for (int j = 0; j < messagesCount; j++) {
              notificationService.publish(FEED1, new SimpleNotification(0, String.format("fake-payload-%d", j)));
              TimeUnit.MILLISECONDS.sleep(r.nextInt(200));
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread1 = new Thread(publisherRunnable1);
      publisherThread1.start();

      Runnable publisherRunnable2 = new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            for (int j = 0; j < messagesCount; j++) {
              notificationService.publish(FEED2, String.format("fake-payload-%d", j));
              TimeUnit.MILLISECONDS.sleep(r.nextInt(200));
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread2 = new Thread(publisherRunnable2);
      publisherThread2.start();

      publisherThread1.join();
      publisherThread2.join();
      TimeUnit.MILLISECONDS.sleep(2000);
      for (Cancellable cancellable : cancellables) {
        cancellable.cancel();
      }

      Assert.assertTrue(assertionOk.get());
      for (int i : receiveCounts) {
        Assert.assertEquals(messagesCount, i);
      }
    } finally {
      feedClient.deleteFeed(FEED1);
      feedClient.deleteFeed(FEED2);
    }
  }

  @Test
  public void twoFeedsPublishOneFeedSubscribeTest() throws Exception {
    // Test two publishers on two different feeds, but only one subscriber subscribing to one of the feeds

    final int messagesCount = 15;
    Assert.assertTrue(feedClient.createFeed(FEED1));
    Assert.assertTrue(feedClient.createFeed(FEED2));
    try {

      // Create a subscribing process
      final AtomicInteger receiveCount = new AtomicInteger(0);
      final AtomicBoolean assertionOk = new AtomicBoolean(true);

      Cancellable cancellable = notificationService.subscribe(FEED1, new NotificationHandler<String>() {
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
            Throwables.propagate(t);
          }
        }
      });
      // Give the subscriber some time to prepare for published messages before starting the publisher
      TimeUnit.MILLISECONDS.sleep(500);

      Runnable publisherRunnable1 = new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            for (int j = 0; j < messagesCount; j++) {
              notificationService.publish(FEED1, String.format("fake-payload-%d", j));
              TimeUnit.MILLISECONDS.sleep(r.nextInt(200));
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread1 = new Thread(publisherRunnable1);
      publisherThread1.start();

      Runnable publisherRunnable2 = new Runnable() {
        @Override
        public void run() {
          try {
            Random r = new Random();
            for (int j = 0; j < messagesCount; j++) {
              notificationService.publish(FEED2, String.format("fake-payload2-%d", j));
              TimeUnit.MILLISECONDS.sleep(r.nextInt(200));
            }
          } catch (Throwable e) {
            Throwables.propagate(e);
          }
        }
      };
      Thread publisherThread2 = new Thread(publisherRunnable2);
      publisherThread2.start();

      publisherThread1.join();
      publisherThread2.join();
      TimeUnit.MILLISECONDS.sleep(2000);
      cancellable.cancel();

      Assert.assertTrue(assertionOk.get());
      Assert.assertEquals(messagesCount, receiveCount.get());
    } finally {
      feedClient.deleteFeed(FEED1);
      feedClient.deleteFeed(FEED2);
    }
  }

  private static final class SimpleNotification {
    private final int publisherId;
    private final String payload;

    private SimpleNotification(int publisherId, String payload) {
      this.publisherId = publisherId;
      this.payload = payload;
    }

    public int getPublisherId() {
      return publisherId;
    }

    public String getPayload() {
      return payload;
    }

    @Override
    public String toString() {
      return String.format("id: %d, payload: %s", publisherId, payload);
    }
  }
}
