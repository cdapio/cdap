/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.notifications.NotificationTest;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests notifications using Kafka transport system.
 */
public class KafkaNotificationTest extends NotificationTest {

  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester(
    ImmutableMap.of(Constants.Notification.TRANSPORT_SYSTEM, "kafka"),
    Iterables.concat(
      getCommonModules(),
      ImmutableList.of(
        new NotificationServiceRuntimeModule().getDistributedModules(),
        new NotificationFeedServiceRuntimeModule().getDistributedModules()
      )
    ),
    1
  );

  @BeforeClass
  public static void start() throws Exception {
    startServices(KAFKA_TESTER.getInjector());

    // TODO remove once Twill addLatest bug is fixed
    feedManager.createFeed(FEED1_INFO);
    feedManager.createFeed(FEED2_INFO);

    // Try to publish to the feeds. Needs to retry multiple times due to race between Kafka server registers itself
    // to ZK and the publisher be able to see the changes in the ZK to get the broker list
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          getNotificationService().publish(FEED1, "test").get();
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          getNotificationService().publish(FEED2, "test").get();
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    feedManager.deleteFeed(FEED1);
    feedManager.deleteFeed(FEED2);
  }

  @AfterClass
  public static void shutDown() throws Exception {
    stopServices();
  }
}
