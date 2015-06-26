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

package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link KafkaIngestionApp}.
 */
public class KafkaIngestionAppTest extends TestBase {
  private static final String KAFKA_TOPIC = "someTopic";
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static InMemoryZKServer zkServer;
  protected static EmbeddedKafkaServer kafkaServer;
  protected static int kafkaPort;

  @BeforeClass
  public static void initialize() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(), kafkaPort,
                                                              TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void cleanup() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("kafka.topic", KAFKA_TOPIC);
    runtimeArgs.put("kafka.zookeeper", zkServer.getConnectionStr());

    // Deploy the KafkaIngestionApp application
    ApplicationManager appManager = deployApplication(KafkaIngestionApp.class);
    appManager.startFlow(Constants.FLOW_NAME, runtimeArgs);
    ServiceManager serviceManager = appManager.startService(Constants.SERVICE_NAME);
    serviceManager.waitForStatus(true);

    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);
    KafkaPublisher.Preparer preparer = publisher.prepare(KAFKA_TOPIC);

    for (int i = 0; i < 10; i++) {
      preparer.add(Charsets.UTF_8.encode("message" + i), i);
    }
    preparer.send();

    RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(KafkaIngestionApp.class.getSimpleName(),
                                                                 Constants.FLOW_NAME,
                                                                 Constants.COUNTER_FLOWLET);
    countMetrics.waitForProcessed(10, 10, TimeUnit.SECONDS);
    try {
      URL serviceURL = serviceManager.getServiceURL();
      URL url = new URL(serviceURL, "v1/avgSize");
      HttpRequest request = HttpRequest.get(url).build();
      HttpResponse response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals("8", response.getResponseBodyAsString());
    } finally {
      serviceManager.stop();
      serviceManager.waitForStatus(false);
    }
    appManager.stopAll();
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }
}
