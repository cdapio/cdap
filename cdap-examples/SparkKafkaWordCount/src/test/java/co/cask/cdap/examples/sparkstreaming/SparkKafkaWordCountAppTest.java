package co.cask.cdap.examples.sparkstreaming;

import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
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
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Created by rsinha on 4/19/16.
 */
public class SparkKafkaWordCountAppTest extends TestBase {

  private static final String KAFKA_TOPIC = "someTopic";
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static InMemoryZKServer zkServer;
  protected static EmbeddedKafkaServer kafkaServer;
  protected static int kafkaPort;

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
    runtimeArgs.put("kafka.topic", KAFKA_TOPIC);
    runtimeArgs.put("kafka.zookeeper", zkServer.getConnectionStr());

    // Deploy the KafkaIngestionApp application
    ApplicationManager appManager = deployApplication(SparkKafkaWordCountApp.class);
    appManager.getSparkManager("SparkKafkaWordCountProgram").start(runtimeArgs);

    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);
    KafkaPublisher.Preparer preparer = publisher.prepare(KAFKA_TOPIC);

    for (int i = 0; i < 10; i++) {
      preparer.add(Charsets.UTF_8.encode("message" + i), i);
    }
    preparer.send();

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
