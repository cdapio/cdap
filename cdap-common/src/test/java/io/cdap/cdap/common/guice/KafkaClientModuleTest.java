/*
 * Copyright © 2017-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.KafkaConstants;
import io.cdap.cdap.common.utils.Tasks;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for the {@link KafkaClientModule}.
 */
public class KafkaClientModuleTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private InMemoryZKServer zkServer;
  private EmbeddedKafkaServer kafkaServer;
  private String kafkaZkConnect;

  @Before
  public void beforeTest() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAsync().awaitRunning();

    CConfiguration cConf = CConfiguration.create();
    String kafkaZkNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    kafkaZkConnect = zkServer.getConnectionStr();

    if (kafkaZkNamespace != null) {
      ZKClientService zkClient = new DefaultZKClientService(zkServer.getConnectionStr(), 2000, null,
          ImmutableMultimap.of());
      zkClient.startAsync().awaitRunning();
      zkClient.create("/" + kafkaZkNamespace, null, CreateMode.PERSISTENT);
      zkClient.stopAsync().awaitTerminated();

      kafkaZkConnect += "/" + kafkaZkNamespace;
    }

    kafkaServer = createKafkaServer(kafkaZkConnect, TEMP_FOLDER.newFolder());
    kafkaServer.startAsync().awaitRunning();
  }

  @After
  public void afterTest() {
    kafkaServer.stopAsync().awaitTerminated();
    zkServer.stopAsync().awaitTerminated();
  }

  @Test
  public void testWithSharedZkClient() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new ZkClientModule(),
        new KafkaClientModule()
    );

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAsync().awaitRunning();

    final int baseZkConns = getZkConnections();

    KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
    KafkaClient kafkaClient = injector.getInstance(KafkaClient.class);
    Assert.assertSame(kafkaClientService, kafkaClient);
    final BrokerService brokerService = injector.getInstance(BrokerService.class);

    kafkaClientService.startAsync().awaitRunning();
    brokerService.startAsync().awaitRunning();

    Assert.assertTrue(zkClientService.isRunning());
    Assert.assertEquals(baseZkConns, getZkConnections());

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return brokerService.getBrokers().iterator().hasNext();
      }
    }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    kafkaClientService.stopAsync().awaitTerminated();
    brokerService.stopAsync().awaitTerminated();

    Assert.assertTrue(zkClientService.isRunning());
    Assert.assertEquals(baseZkConns, getZkConnections());

    zkClientService.stopAsync().awaitTerminated();
  }

  @Test
  public void testWithDedicatedZkClient() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM, kafkaZkConnect);

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new ZkClientModule(),
        new KafkaClientModule()
    );

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAsync().awaitRunning();

    int baseZkConns = getZkConnections();

    KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
    final BrokerService brokerService = injector.getInstance(BrokerService.class);

    kafkaClientService.startAsync().awaitRunning();
    Assert.assertEquals(baseZkConns + 1, getZkConnections());

    brokerService.startAsync().awaitRunning();
    Assert.assertEquals(baseZkConns + 1, getZkConnections());

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return brokerService.getBrokers().iterator().hasNext();
      }
    }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    Assert.assertTrue(zkClientService.isRunning());

    brokerService.stopAsync().awaitTerminated();
    Assert.assertEquals(baseZkConns + 1, getZkConnections());

    kafkaClientService.stopAsync().awaitTerminated();
    Assert.assertEquals(baseZkConns, getZkConnections());

    Assert.assertTrue(zkClientService.isRunning());

    zkClientService.stopAsync().awaitTerminated();
  }

  @Test
  public void testDedicatedZkClientReverseStopOrder() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.set(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM, kafkaZkConnect);

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new ZkClientModule(),
        new KafkaClientModule()
    );

    KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
    BrokerService brokerService = injector.getInstance(BrokerService.class);

    brokerService.startAsync().awaitRunning();
    kafkaClientService.startAsync().awaitRunning();

    // Stop kafkaClientService first while brokerService still holds a ref count > 0.
    kafkaClientService.stopAsync().awaitTerminated();
    Assert.assertTrue(brokerService.isRunning());

    brokerService.stopAsync().awaitTerminated();
    Assert.assertFalse(brokerService.isRunning());
  }

  private int getZkConnections() throws IOException {
    InetSocketAddress zkAddr = zkServer.getLocalAddress();
    try (Socket socket = new Socket(zkAddr.getAddress(), zkAddr.getPort())) {
      socket.getOutputStream().write("cons".getBytes(StandardCharsets.ISO_8859_1));
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),
          StandardCharsets.ISO_8859_1));
      String line = reader.readLine();
      int count = 0;
      while (line != null) {
        count++;
        line = reader.readLine();
      }

      return count;
    }
  }

  private EmbeddedKafkaServer createKafkaServer(String zkConnectStr, File dir) {
    Properties properties = new Properties();
    properties.setProperty("num.network.threads", "2");
    properties.setProperty("num.io.threads", "2");
    properties.setProperty("socket.send.buffer.bytes", "1048576");
    properties.setProperty("socket.receive.buffer.bytes", "1048576");
    properties.setProperty("socket.request.max.bytes", "104857600");
    properties.setProperty("log.dir", dir.getAbsolutePath());
    properties.setProperty("num.partitions", "1");
    properties.setProperty("log.flush.interval.messages", "10000");
    properties.setProperty("log.flush.interval.ms", "1000");
    properties.setProperty("log.retention.hours", "1");
    properties.setProperty("log.segment.bytes", "536870912");
    properties.setProperty("log.cleanup.interval.mins", "1");
    properties.setProperty("zookeeper.connect", zkConnectStr);
    properties.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return new EmbeddedKafkaServer(properties);
  }
}
