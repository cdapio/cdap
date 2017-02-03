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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SpamClassifier}
 */
public class SpamClassifierTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final String KAFKA_TOPIC = "someTopic";
  private static final String KAFKA_BROKER_ID = "1";
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static int kafkaPort;

  @BeforeClass
  public static void init() throws Exception {
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
  public void test() throws Exception {

    // Deploy the KafkaIngestionApp application
    ApplicationManager appManager = deployApplication(SpamClassifier.class);

    ingestTrainingData();

    publishKafkaMessages();

    // start spark streaming program
    SparkManager sparkManager = appManager.getSparkManager(SpamClassifierProgram.class.getSimpleName());
    Map<String, String> runtimeArgs = new HashMap<>();
    runtimeArgs.put("kafka.brokers", "127.0.0.1:" + kafkaPort);
    runtimeArgs.put("kafka.topics", KAFKA_TOPIC);
    sparkManager.start(runtimeArgs);

    // Start and wait for service to start
    final ServiceManager serviceManager = appManager.getServiceManager(SpamClassifier.SERVICE_HANDLER).start();
    serviceManager.waitForStatus(true);

    // wait for spark streaming program to write to dataset
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return testClassification(serviceManager, "1", SpamClassifier.SpamClassifierServiceHandler.SPAM) &&
          testClassification(serviceManager, "2", SpamClassifier.SpamClassifierServiceHandler.HAM);
      }
    }, 60, TimeUnit.SECONDS);

    // stop spark program
    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 1, TimeUnit.MINUTES);

    appManager.stopAll();
  }

  private boolean testClassification(ServiceManager serviceManager, String messageId, String expected) throws
    IOException {
    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                      SpamClassifier.SpamClassifierServiceHandler.CLASSIFICATION_PATH + "/" + messageId);
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    return (HttpURLConnection.HTTP_OK == response.getResponseCode() &&
      expected.equalsIgnoreCase(response.getResponseBodyAsString()));
  }

  private void publishKafkaMessages() {
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);
    KafkaPublisher.Preparer preparer = publisher.prepare(KAFKA_TOPIC);

    preparer.add(Charsets.UTF_8.encode("1:REMINDER FROM O2: To get 2.50 pounds free call credit and details of great " +
                                         "offers pls reply 2 this text with your valid name, house no and postcode"),
                 "1"); // spam
    preparer.add(Charsets.UTF_8.encode("2:I will call you later"), "2"); // ham
    preparer.send();
  }

  private void ingestTrainingData() throws IOException {
    StreamManager streamManager = getStreamManager(SpamClassifier.STREAM);

    try (BufferedReader reader = new BufferedReader(
      new InputStreamReader(getClass().getResourceAsStream("/trainingData.txt"), "UTF-8"))) {
      String line = reader.readLine();
      while (line != null) {
        streamManager.send(line);
        line = reader.readLine();
      }
    }
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
