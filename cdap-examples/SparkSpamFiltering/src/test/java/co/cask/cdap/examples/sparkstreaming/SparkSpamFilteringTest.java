/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.examples.sparkstreaming;

import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
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
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link SparkSpamFiltering}
 */
public class SparkSpamFilteringTest extends TestBase {

  private static final String KAFKA_TOPIC = "someTopic";
  private static final String KAFKA_BROKER_ID = "1";
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void initialize() throws Exception {
    TestBase.initialize();
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
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("kafka.brokers", "127.0.0.1:" + kafkaPort);
    runtimeArgs.put("kafka.topics", KAFKA_TOPIC);

    System.out.println("Kakfa port is " + kafkaPort);

    // Deploy the KafkaIngestionApp application
    ApplicationManager appManager = deployApplication(SparkSpamFiltering.class);
    // Send events to the Stream
    StreamManager streamManager = getStreamManager(SparkSpamFiltering.STREAM);
    BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/spam.txt"),
                                                                     "UTF-8"));
    try {
      String line = reader.readLine();
      while (line != null) {
        streamManager.send(line);
        line = reader.readLine();
      }
    } finally {
      Closeables.closeQuietly(reader);
    }

    SparkManager sparkManager = appManager.getSparkManager("SparkSpamFilteringProgram");
    System.out.println("here1");
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);
    KafkaPublisher.Preparer preparer = publisher.prepare(KAFKA_TOPIC);

    preparer.add(Charsets.UTF_8.encode("REMINDER FROM O2: To get 2.50 pounds free call credit and details of great " +
                                         "offers pls reply 2 this text with your valid name, house no and postcode"),
                 "spam"); // spam
    preparer.add(Charsets.UTF_8.encode("I will call you later"), "not-spam"); // not spam

    preparer.send();

    sparkManager.start(runtimeArgs);
    TimeUnit.SECONDS.sleep(60);

    sparkManager.stop();
    sparkManager.waitForFinish(10, TimeUnit.SECONDS);

    // Start service
    ServiceManager serviceManager = appManager.getServiceManager(SparkSpamFiltering.SERVICE_HANDLER)
      .start();

    // Wait for service to start
    serviceManager.waitForStatus(true);

    // Request total pages for a page rank and verify it
    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                  SparkSpamFiltering.SparkSpamFilteringServiceHandler.CLASSIFICATION_PATH + "/" + Math.abs("spam".hashCode()));
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("Spam", response.getResponseBodyAsString());

    // Request total pages for a page rank and verify it
    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                      SparkSpamFiltering.SparkSpamFilteringServiceHandler.CLASSIFICATION_PATH + "/" + "not-spam");
    response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals("Not spam", response.getResponseBodyAsString());

    appManager.stopAll();
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("host.name", "127.0.0.1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", KAFKA_BROKER_ID);
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
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
