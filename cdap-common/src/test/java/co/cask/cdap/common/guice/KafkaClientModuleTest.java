/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.utils.Tasks;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.zookeeper.DefaultZKClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

/**
 * Unit test for the {@link KafkaClientModule}.
 */
public class KafkaClientModuleTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private InMemoryZKServer zkServer;
  private EmbeddedKafkaServer kafkaServer;
  private String kafkaZKConnect;

  @Before
  public void beforeTest() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    kafkaZKConnect = zkServer.getConnectionStr();

    if (kafkaZKNamespace != null) {
      ZKClientService zkClient = new DefaultZKClientService(zkServer.getConnectionStr(), 2000, null,
                                                            ImmutableMultimap.<String, byte[]>of());
      zkClient.startAndWait();
      zkClient.create("/" + kafkaZKNamespace, null, CreateMode.PERSISTENT);
      zkClient.stopAndWait();

      kafkaZKConnect += "/" + kafkaZKNamespace;
    }

    kafkaServer = createKafkaServer(kafkaZKConnect, TEMP_FOLDER.newFolder());
    kafkaServer.startAndWait();
  }

  @After
  public void afterTest() {
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testWithSharedZKClient() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new KafkaClientModule()
    );

    // Get the shared zkclient and start it
    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    int baseZKConns = getZKConnections();

    KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
    final BrokerService brokerService = injector.getInstance(BrokerService.class);

    // Start both kafka and broker services, it shouldn't affect the state of the shared zk client
    kafkaClientService.startAndWait();
    brokerService.startAndWait();

    // Shouldn't affect the shared zk client state
    Assert.assertTrue(zkClientService.isRunning());

    // It shouldn't increase the number of zk client connections
    Assert.assertEquals(baseZKConns, getZKConnections());

    // Make sure it is talking to Kafka.
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerService.getBrokers().iterator().hasNext();
      }
    }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Stop both, still shouldn't affect the state of the shared zk client
    kafkaClientService.stopAndWait();
    brokerService.stopAndWait();

    // Still shouldn't affect the shared zk client
    Assert.assertTrue(zkClientService.isRunning());

    // It still shouldn't increase the number of zk client connections
    Assert.assertEquals(baseZKConns, getZKConnections());

    zkClientService.stopAndWait();
  }

  @Test
  public void testWithDedicatedZKClient() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    // Set the zk quorum for the kafka client, expects it to create and start/stop it's own zk client service
    cConf.set(KafkaConstants.ConfigKeys.ZOOKEEPER_QUORUM, kafkaZKConnect);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new KafkaClientModule()
    );

    // Get the shared zkclient and start it
    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    int baseZKConns = getZKConnections();

    KafkaClientService kafkaClientService = injector.getInstance(KafkaClientService.class);
    final BrokerService brokerService = injector.getInstance(BrokerService.class);

    // Start the kafka client, it should increase the zk connections by 1
    kafkaClientService.startAndWait();
    Assert.assertEquals(baseZKConns + 1, getZKConnections());

    // Start the broker service,
    // it shouldn't affect the zk connections, as it share the same zk client with kafka client
    brokerService.startAndWait();
    Assert.assertEquals(baseZKConns + 1, getZKConnections());

    // Make sure it is talking to Kafka.
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return brokerService.getBrokers().iterator().hasNext();
      }
    }, 5L, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);


    // Shouldn't affect the shared zk client state
    Assert.assertTrue(zkClientService.isRunning());

    // Stop the broker service, it shouldn't affect the zk connections, as it is still used by the kafka client
    brokerService.stopAndWait();
    Assert.assertEquals(baseZKConns + 1, getZKConnections());

    // Stop the kafka client, the zk connections should be reduced by 1
    kafkaClientService.stopAndWait();
    Assert.assertEquals(baseZKConns, getZKConnections());

    // Still shouldn't affect the shared zk client
    Assert.assertTrue(zkClientService.isRunning());

    zkClientService.stopAndWait();
  }

  /**
   * Returns the number of client connections to the zk server
   */
  private int getZKConnections() throws IOException {
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

  private EmbeddedKafkaServer createKafkaServer(String zkConnectStr, File dir) throws Exception {
    // Don't set port, EmbeddedKafkaServer will find a ephemeral port.
    Properties properties = new Properties();
    properties.setProperty("broker.id", "1");
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
