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

package co.cask.cdap.notifications.kafka;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.notifications.NotificationTest;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaNotificationTest extends NotificationTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static EmbeddedKafkaServer kafkaServer;

  @BeforeClass
  public static void start() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    cConf.unset(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(Constants.Notification.TRANSPORT_SYSTEM, "kafka");

    Injector injector = createInjector(
      cConf,
      new ZKClientModule(),
      new KafkaClientModule(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules()
    );

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();

    startServices(injector);

    Properties kafkaConfig = generateKafkaConfig(zkServer, tmpFolder.newFolder("kafka-notifications-test"));

    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();

    // TODO remove once Twill addLatest bug is fixed
    feedManager.createFeed(FEED1);
    feedManager.createFeed(FEED2);

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
    kafkaClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  private static Properties generateKafkaConfig(InMemoryZKServer zkServer, File tmpFolder) {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", "1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("num.network.threads", "2");
    prop.setProperty("num.io.threads", "2");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", tmpFolder.getAbsolutePath());
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }

}
