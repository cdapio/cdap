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

package co.cask.cdap.data.stream.heartbeat;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.stream.notification.StreamSizeNotification;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data.stream.StreamLeaderListener;
import co.cask.cdap.data.stream.service.heartbeat.NotificationHeartbeatsAggregator;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data.stream.service.heartbeat.StreamsHeartbeatsAggregator;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public abstract class NotificationHeartbeatsAggregatorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregatorTestBase.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static NotificationFeedManager feedManager;

  private static StreamCoordinator streamCoordinator;
  private static NotificationService notificationService;
  private static StreamsHeartbeatsAggregator aggregator;

  protected abstract StreamAdmin getStreamAdmin();

  public static Injector createInjector(CConfiguration cConf, Configuration hConf,  Module... modules) {

    return Guice.createInjector(Iterables.concat(
      ImmutableList.of(new ConfigModule(cConf, hConf),
                       new DiscoveryRuntimeModule().getInMemoryModules(),
                       new DataSetsModules().getLocalModule(),
                       new DataSetServiceModules().getInMemoryModule(),
                       new ExploreClientModule(),
                       new IOModule(),
                       new AuthModule(),
                       new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
                       new NotificationServiceRuntimeModule().getInMemoryModules(),
                       new AbstractModule() {
                         @Override
                         protected void configure() {
                           bind(StreamsHeartbeatsAggregator.class)
                             .to(NotificationHeartbeatsAggregator.class).in(Scopes.SINGLETON);
                         }
                       }),
      Arrays.asList(modules)));
  }

  public static void startServices(Injector injector) throws Exception {
    aggregator = injector.getInstance(StreamsHeartbeatsAggregator.class);
    aggregator.startAndWait();

    streamCoordinator = injector.getInstance(StreamCoordinator.class);
    streamCoordinator.addLeaderListener(new StreamLeaderListener() {
      @Override
      public void leaderOf(Set<String> streamNames) {
        aggregator.listenToStreams(streamNames);
      }
    });
    streamCoordinator.startAndWait();

    notificationService = injector.getInstance(NotificationService.class);
    notificationService.startAndWait();

    feedManager = injector.getInstance(NotificationFeedManager.class);
  }

  public static void stopServices() throws Exception {
    aggregator.stopAndWait();
    notificationService.stopAndWait();
    streamCoordinator.stopAndWait();
  }

  @Category(SlowTests.class)
  @Test
  public void testAggregator() throws Exception {
    String streamName = "stream";
    StreamAdmin streamAdmin = getStreamAdmin();
    long partitionDuration = 3600;

    // Create a stream with 3600 seconds partition.
    Properties properties = new Properties();
    properties.setProperty(Constants.Stream.PARTITION_DURATION, Long.toString(partitionDuration));
    streamAdmin.create(streamName, properties);

    NotificationFeed heartbeatFeed = new NotificationFeed.Builder()
      .setNamespace("default")
      .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
      .setName(streamName)
      .build();
    NotificationFeed streamFeed = new NotificationFeed.Builder(heartbeatFeed)
      .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
      .build();

    // Check that the feeds where persisted when the stream was created - exceptions will be thrown if not
    feedManager.getFeed(heartbeatFeed);
    feedManager.getFeed(streamFeed);

    // Assert than one notification will be sent to notify stream size change
    final AtomicBoolean notificationReceived = new AtomicBoolean(false);
    final AtomicBoolean assertionOk = new AtomicBoolean(true);
    Cancellable cancellable = notificationService.subscribe(streamFeed, new NotificationHandler<Object>() {
      @Override
      public Type getNotificationFeedType() {
        return StreamSizeNotification.class;
      }

      @Override
      public void received(Object notification, NotificationContext notificationContext) {
        try {
          LOG.info("Notification {} received", notification);
          Assert.assertTrue(notificationReceived.compareAndSet(false, true));
        } catch (Throwable t) {
          assertionOk.set(false);
          Throwables.propagate(t);
        }
      }
    });

    // Send fake heartbeats describing large increments of data
    long increment = Constants.Notification.Stream.DEFAULT_DATA_THRESHOLD / 3;
    for (int i = 0; i <= 3; i++) {
      notificationService.publish(heartbeatFeed,
                                  new StreamWriterHeartbeat(System.currentTimeMillis(), increment, i,
                                                            StreamWriterHeartbeat.Type.REGULAR))
        .get();
    }

    // Wait for the aggregator to do its logic and for the subscriber to receive the stream notification
    TimeUnit.SECONDS.sleep(Constants.Notification.Stream.AGGREGATION_DELAY + 2);
    cancellable.cancel();
    Assert.assertTrue(assertionOk.get());
    Assert.assertTrue(notificationReceived.get());
  }
}
