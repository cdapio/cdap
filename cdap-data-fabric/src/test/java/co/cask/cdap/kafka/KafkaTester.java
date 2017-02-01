/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.kafka;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.utils.Tasks;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExternalResource} to be used in tests that require in-memory Kafka.
 */
public class KafkaTester extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTester.class);
  private static final Gson GSON = new Gson();

  private Injector injector;
  private InMemoryZKServer zkServer;
  private EmbeddedKafkaServer kafkaServer;
  private ZKClientService zkClient;
  private BrokerService brokerService;
  private KafkaClientService kafkaClient;

  private final Map<String, String> extraConfigs;
  private final Iterable<Module> extraModules;
  private final CConfiguration cConf;
  private final String [] kafkaBrokerListParams;
  private final TemporaryFolder tmpFolder;
  private final int numPartitions;

  /**
   * Create a {@link KafkaTester} with default configurations and {@link Module Guice modules} and a single
   * Kafka partition.
   */
  public KafkaTester() {
    this(ImmutableMap.<String, String>of(), ImmutableList.<Module>of(), 1);
  }

  /**
   * Create a {@link KafkaTester} with the specified set of extra configurations, {@link Module Guice modules} and
   * Kafka partitions.
   *
   * @param extraConfigs the specified extra configurations to provide while creating the
   * {@link Injector Guice injector}
   * @param extraModules the specified extra {@link Module Guice modules} to include in the
   * {@link Injector Guice injector}
   * @param numPartitions the specified number of Kafka partitions
   * @param kafkaBrokerListParams a list of configuration parameters to which the kafka broker list should be set
   */
  public KafkaTester(Map<String, String> extraConfigs, Iterable<Module> extraModules, int numPartitions,
                     String ... kafkaBrokerListParams) {
    this.extraConfigs = extraConfigs;
    this.extraModules = extraModules;
    this.cConf = CConfiguration.create();
    this.kafkaBrokerListParams = kafkaBrokerListParams;
    this.tmpFolder = new TemporaryFolder();
    this.numPartitions = numPartitions;
  }

  @Override
  protected void before() throws Throwable {
    int kafkaPort = Networks.getRandomPort();
    Preconditions.checkState(kafkaPort > 0, "Failed to get random port.");
    int zkServerPort = Networks.getRandomPort();
    Preconditions.checkState(zkServerPort > 0, "Failed to get random port.");
    tmpFolder.create();
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).setPort(zkServerPort).build();
    zkServer.startAndWait();
    LOG.info("In memory ZK started on {}", zkServer.getConnectionStr());

    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(kafkaPort));
    kafkaServer.startAndWait();
    initializeCConf(kafkaPort);
    injector = createInjector();
    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();
    brokerService = injector.getInstance(BrokerService.class);
    brokerService.startAndWait();
    LOG.info("Waiting for Kafka server to startup...");
    waitForKafkaStartup();
    LOG.info("Started kafka server on port {}", kafkaPort);
  }

  @Override
  protected void after() {
    brokerService.stopAndWait();
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  private void initializeCConf(int kafkaPort) throws IOException {
    cConf.unset(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    for (Map.Entry<String, String> entry : extraConfigs.entrySet()) {
      cConf.set(entry.getKey(), entry.getValue());
    }
    // Also set kafka broker list in the specified config parameters
    for (String param : kafkaBrokerListParams) {
      cConf.set(param, InetAddress.getLoopbackAddress().getHostAddress() + ":" + kafkaPort);
    }
  }

  private Injector createInjector() throws IOException {
    List<Module> modules = ImmutableList.<Module>builder()
      .add(new ConfigModule(cConf))
      .add(new ZKClientModule())
      .add(new KafkaClientModule())
      .addAll(extraModules)
      .build();

    return Guice.createInjector(modules);
  }

  private Properties generateKafkaConfig(int port) throws IOException {
    Properties properties = new Properties();
    properties.setProperty("broker.id", "1");
    properties.setProperty("port", Integer.toString(port));
    properties.setProperty("num.network.threads", "2");
    properties.setProperty("num.io.threads", "2");
    properties.setProperty("socket.send.buffer.bytes", "1048576");
    properties.setProperty("socket.receive.buffer.bytes", "1048576");
    properties.setProperty("socket.request.max.bytes", "104857600");
    properties.setProperty("log.dir", tmpFolder.newFolder().getAbsolutePath());
    properties.setProperty("num.partitions", String.valueOf(numPartitions));
    properties.setProperty("log.flush.interval.messages", "10000");
    properties.setProperty("log.flush.interval.ms", "1000");
    properties.setProperty("log.retention.hours", "1");
    properties.setProperty("log.segment.bytes", "536870912");
    properties.setProperty("log.cleanup.interval.mins", "1");
    properties.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    properties.setProperty("zookeeper.connection.timeout.ms", "1000000");
    return properties;
  }

  private void waitForKafkaStartup() throws Exception {
    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        final AtomicBoolean isKafkaStarted = new AtomicBoolean(false);
        try {
          KafkaPublisher kafkaPublisher = kafkaClient.getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED,
                                                                   Compression.NONE);
          final String testTopic = "kafkatester.test.topic";
          final String testMessage = "Test Message";
          kafkaPublisher.prepare(testTopic).add(Charsets.UTF_8.encode(testMessage), 0).send().get();
          getPublishedMessages(testTopic, ImmutableSet.of(0), 1, 0, new Function<FetchedMessage, String>() {
            @Override
            public String apply(FetchedMessage input) {
              String fetchedMessage = Charsets.UTF_8.decode(input.getPayload()).toString();
              if (fetchedMessage.equalsIgnoreCase(testMessage)) {
                isKafkaStarted.set(true);
              }
              return "";
            }
          });
        } catch (Exception e) {
          // nothing to do as waiting for kafka startup
        }
        return isKafkaStarted.get();
      }
    }, 60, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  public Injector getInjector() {
    return injector;
  }

  public CConfiguration getCConf() {
    return cConf;
  }

  /**
   * Return a list of messages from the specified Kafka topic.
   *
   * @param topic the specified Kafka topic
   * @param expectedNumMsgs the expected number of messages
   * @param typeOfT the {@link Type} of each message
   * @param <T> the type of each message
   * @return a list of messages from the specified Kafka topic
   */
  @SuppressWarnings("unused")
  public <T> List<T> getPublishedMessages(String topic, int expectedNumMsgs, Type typeOfT) throws InterruptedException {
    return getPublishedMessages(topic, expectedNumMsgs, typeOfT, GSON);
  }

  /**
   * Return a list of messages from the specified Kafka topic.
   *
   * @param topic the specified Kafka topic
   * @param expectedNumMsgs the expected number of messages
   * @param typeOfT the {@link Type} of each message
   * @param gson the {@link Gson} object to use for deserializing messages
   * @param <T> the type of each message
   * @return a list of messages from the specified Kafka topic
   */
  public <T> List<T> getPublishedMessages(String topic, int expectedNumMsgs, Type typeOfT,
                                          Gson gson) throws InterruptedException {
    return getPublishedMessages(topic, expectedNumMsgs, typeOfT, gson, 0);
  }

  /**
   * Return a list of messages from the specified Kafka topic.
   *
   * @param topic the specified Kafka topic
   * @param expectedNumMsgs the expected number of messages
   * @param typeOfT the {@link Type} of each message
   * @param gson the {@link Gson} object to use for deserializing messages
   * @param offset the Kafka offset
   * @param <T> the type of each message
   * @return a list of messages from the specified Kafka topic
   */
  public <T> List<T> getPublishedMessages(String topic, int expectedNumMsgs, final Type typeOfT,
                                          final Gson gson, int offset) throws InterruptedException {
    return getPublishedMessages(topic, ImmutableSet.of(0), expectedNumMsgs, offset, new Function<FetchedMessage, T>() {
      @Override
      public T apply(FetchedMessage input) {
        return gson.fromJson(Bytes.toString(input.getPayload()), typeOfT);
      }
    });
  }

  /**
   * Return a list of messages from the specified Kafka topic.
   *
   * @param topic the specified Kafka topic
   * @param expectedNumMsgs the expected number of messages
   * @param offset the Kafka offset
   * @param converter converter function to convert payload bytebuffer into type T
   * @param <T> the type of each message
   * @return a list of messages from the specified Kafka topic
   */
  public <T> List<T> getPublishedMessages(String topic, Set<Integer> partitions, int expectedNumMsgs, int offset,
                                          final Function<FetchedMessage, T> converter) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(expectedNumMsgs);
    final CountDownLatch stopLatch = new CountDownLatch(1);
    final List<T> actual = new ArrayList<>(expectedNumMsgs);
    KafkaConsumer.Preparer preparer = kafkaClient.getConsumer().prepare();
    for (int partition : partitions) {
      preparer.add(topic, partition, offset);
    }
    Cancellable cancellable = preparer.consume(
      new KafkaConsumer.MessageCallback() {
        @Override
        public long onReceived(Iterator<FetchedMessage> messages) {
          long offset = 0L;
          while (messages.hasNext()) {
            FetchedMessage message = messages.next();
            actual.add(converter.apply(message));
            latch.countDown();
            offset = message.getNextOffset();
          }
          return offset;
        }

        @Override
        public void finished() {
          stopLatch.countDown();
        }
      }
    );
    Assert.assertTrue(String.format("Expected %d messages but found %d messages", expectedNumMsgs, actual.size()),
                      latch.await(15, TimeUnit.SECONDS));
    cancellable.cancel();
    Assert.assertTrue(stopLatch.await(15, TimeUnit.SECONDS));
    return actual;
  }

  /**
   * Returns the {@link KafkaClient} that can be used to talk to the Kafka server started in this class.
   */
  public KafkaClient getKafkaClient() {
    return kafkaClient;
  }

  /**
   * Returns the {@link BrokerService} that provides information about the Kafka server started in this class.
   */
  public BrokerService getBrokerService() {
    return brokerService;
  }

  /**
   * Creates a topic with the given number of partitions.
   */
  public void createTopic(String topic, int partitions) {
    AdminUtils.createTopic(new ZkClient(zkServer.getConnectionStr(), 20000, 2000, ZKStringSerializer$.MODULE$),
                           topic, partitions, 1, new Properties());
  }
}
