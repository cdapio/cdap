/*
 * Copyright © 2020-2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.RuntimeMonitor;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for {@link RuntimeClientService}.
 */
public class RuntimeClientServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int PROGRAM_STATUS_EVENT_TEST_PARTITIONS = 10;
  private static final int METRICS_TEST_PARTITIONS = 2;

  // The cConf value for the runtime monitor topic configs to have two topics, and one has to be program status event
  // This is for testing the RuntimeClientService handling of program status correctly
  private static final String TOPIC_CONFIGS_VALUE =
      Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC
          + "," + Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC + ":"
          + PROGRAM_STATUS_EVENT_TEST_PARTITIONS
          + "," + Constants.Metadata.MESSAGING_TOPIC
          + "," + Constants.Metrics.TOPIC_PREFIX + ":" + METRICS_TEST_PARTITIONS
          + "," + Constants.Audit.TOPIC;

  private static final ProgramRunId PROGRAM_RUN_ID =
      NamespaceId.DEFAULT.app("app").workflow("workflow")
          .run(RunIds.generate());
  private static final Gson GSON = new Gson();

  private List<String> topicNames;
  private List<String> nonStatusTopicNames;

  // Services for the runtime server side
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;

  // Services for the runtime client side
  private CConfiguration clientCConf;
  private MessagingService clientMessagingService;
  private RuntimeClientService runtimeClientService;
  private ProgramStatePublisher clientProgramStatePublisher;

  private AtomicBoolean runCompleted = new AtomicBoolean(false);
  private Injector clientInjector;

  @Before
  public void beforeTest() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.CFG_LOCAL_DATA_DIR,
        TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, TOPIC_CONFIGS_VALUE);
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS,
        PROGRAM_STATUS_EVENT_TEST_PARTITIONS);

    topicNames = RuntimeMonitors.createTopicNameList(cConf);
    Pattern statusTopicRegex = Pattern.compile(
        "^" + Pattern.quote(
            cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC))
            + "[0-9]*$");
    nonStatusTopicNames = topicNames.stream()
        .filter(name -> !statusTopicRegex.matcher(name).matches())
        .collect(Collectors.toList());

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

    // Injector for the server side
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new LocalLocationModule(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new AuthenticationContextModules().getNoOpModule(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new RuntimeServerModule() {
          @Override
          protected void bindRequestValidator() {
            bind(RuntimeRequestValidator.class).toInstance(
                (programRunId, request) -> {
                  if (runCompleted.get()) {
                    throw new GoneException();
                  }
                  return new ProgramRunInfo(ProgramRunStatus.RUNNING);
                });
          }

          @Override
          protected void bindLogProcessor() {
            bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
            });
          }
        },
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(
                NoOpMetricsCollectionService.class);
            bind(DiscoveryService.class).toInstance(discoveryService);
            bind(DiscoveryServiceClient.class).toInstance(discoveryService);
          }
        }
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }

    runtimeServer = injector.getInstance(RuntimeServer.class);
    runtimeServer.startAndWait();

    // Injector for the client side
    clientCConf = CConfiguration.create();
    clientCConf.set(Constants.CFG_LOCAL_DATA_DIR,
        TEMP_FOLDER.newFolder().getAbsolutePath());
    clientCConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS,
        TOPIC_CONFIGS_VALUE);
    clientCConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS,
        PROGRAM_STATUS_EVENT_TEST_PARTITIONS);

    // Shorten the poll delay and grace period to speed up testing of program terminate state handling
    clientCConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 200);
    clientCConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 3000);
    // Use smaller batch size so that fetches is broken into multiple fetches
    clientCConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 1);

    clientInjector = Guice.createInjector(
        new ConfigModule(clientCConf),
        RemoteAuthenticatorModules.getNoOpModule(),
        new MessagingServerRuntimeModule().getInMemoryModules(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new AuthenticationContextModules().getNoOpModule(),
        new IOModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ProgramStatePublisher.class).to(
                MessagingProgramStatePublisher.class);
            bind(MetricsCollectionService.class).to(
                NoOpMetricsCollectionService.class);
            bind(DiscoveryService.class).toInstance(discoveryService);
            bind(DiscoveryServiceClient.class).toInstance(discoveryService);
            bind(ProgramRunId.class).toInstance(PROGRAM_RUN_ID);
          }
        }
    );

    clientMessagingService = clientInjector.getInstance(MessagingService.class);
    if (clientMessagingService instanceof Service) {
      ((Service) clientMessagingService).startAndWait();
    }
    clientProgramStatePublisher = clientInjector.getInstance(
        ProgramStatePublisher.class);
  }

  @After
  public void afterTest() {
    if (runtimeClientService != null) {
      runtimeClientService.stopAndWait();
    }
    runtimeClientService = null;
    if (clientMessagingService instanceof Service) {
      ((Service) clientMessagingService).stopAndWait();
    }

    runtimeServer.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testBasicRelay() throws Exception {
    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    runtimeClientService.startAndWait();
    // Send some messages to multiple topics in the client side TMS, they should get replicated to the server side TMS.
    MessagingContext messagingContext = new MultiThreadMessagingContext(
        clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    // For program status event topic, we need to send valid program status event because
    // the RuntimeClientService will decode it to watch for program termination
    programStateWriter.running(PROGRAM_RUN_ID, null);

    for (String topic : nonStatusTopicNames) {
      messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
          "msg" + topic, "msg" + topic);
    }

    // Extract the program run status from the Notification
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(
        messagingService);
    waitForStatus(serverMessagingContext, ProgramRunStatus.RUNNING);

    for (String topic : nonStatusTopicNames) {
      Tasks.waitFor(Arrays.asList("msg" + topic, "msg" + topic),
          () -> fetchMessages(serverMessagingContext, topic, 10, null)
              .stream().map(Message::getPayloadAsString)
              .collect(Collectors.toList()),
          5, TimeUnit.SECONDS);
    }

    for (String topic : nonStatusTopicNames) {
      Tasks.waitFor(Arrays.asList("msg" + topic, "msg" + topic),
          () -> fetchMessages(serverMessagingContext, topic, 10, null)
              .stream().map(Message::getPayloadAsString)
              .collect(Collectors.toList()),
          5, TimeUnit.SECONDS);
    }

    // Writes a program terminate message to unblock stopping of the client service
    programStateWriter.completed(PROGRAM_RUN_ID);
  }

  @Test
  public void testRelayWithAggregation() throws Exception {
    TypeToken<MetricValues> metricValueType = TypeToken.of(MetricValues.class);
    Schema schema = clientInjector.getInstance(SchemaGenerator.class).generate(
        metricValueType.getType());
    // Use larger batch size so that fetches have enough metrics to aggregate.
    clientCConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 10);
    clientCConf.setBoolean(Constants.RuntimeMonitor.METRICS_AGGREGATION_ENABLED,
        true);
    clientCConf.setLong(
        Constants.RuntimeMonitor.METRICS_AGGREGATION_WINDOW_SECONDS, 10);
    clientCConf.setLong(RuntimeMonitor.METRICS_AGGREGATION_POLL_TIME_MS, 200);

    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    runtimeClientService.startAndWait();
    Map<String, String> tags = new HashMap<>();
    tags.put("key1", "value1");
    tags.put("key2", "value2");
    tags = Collections.unmodifiableMap(tags);

    long aggregationWindowSize = 10;
    // Metrics will be split into two windows:
    // 1. [currentTime - windowSize + 1, currentTime]
    // 2. [currentTime - 2 * windowSize + 1, currentTime - windowSize]
    long currentTimestamp = System.currentTimeMillis() / 1000;
    long windowStartTime = currentTimestamp - 2 * aggregationWindowSize + 1;

    MetricValues counter1 = new MetricValues(tags, "metric1",
        windowStartTime + 2, 1,
        MetricType.COUNTER);
    MetricValues counter2 = new MetricValues(tags, "metric1",
        windowStartTime + 3, 2,
        MetricType.COUNTER);
    // counter3 should not be aggregated with other metrics due to the large time difference.
    MetricValues counter3 = new MetricValues(tags, "metric1", currentTimestamp,
        1, MetricType.COUNTER);

    MetricValues gauge1 = new MetricValues(tags, "metric2", windowStartTime + 4,
        1,
        MetricType.GAUGE);
    MetricValues gauge2 = new MetricValues(tags, "metric2", windowStartTime + 5,
        2,
        MetricType.GAUGE);

    DatumWriter<MetricValues> writer = clientInjector.getInstance(
        DatumWriterFactory.class).create(metricValueType, schema);
    List<byte[]> payloads = Stream.of(counter1, counter2, counter3, gauge1,
            gauge2)
        .map(
            (MetricValues m) -> MetricsMessageAggregatorTest.encodeMetricValues(
                m, writer))
        .collect(Collectors.toList());
    String topic = clientCConf.get(Constants.Metrics.TOPIC_PREFIX) + "0";

    MessagingContext messagingContext = new MultiThreadMessagingContext(
        clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
        payloads.iterator());

    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    // For program status event topic, we need to send valid program status event because
    // the RuntimeClientService will decode it to watch for program termination
    programStateWriter.running(PROGRAM_RUN_ID, null);
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(
        messagingService);
    waitForStatus(serverMessagingContext, ProgramRunStatus.RUNNING);

    Map<String, String> finalTags = tags;
    DatumReader<MetricValues> reader = clientInjector.getInstance(
            DatumReaderFactory.class)
        .create(metricValueType, schema);
    Tasks.waitFor(true, () -> {
      List<MetricValues> metricsList =
          fetchMessages(serverMessagingContext, topic, 10, null)
              .stream()
              .map(
                  (Message m) -> MetricsMessageAggregatorTest.decodeMetricValues(
                      m.getPayload(), reader,
                      schema))
              .collect(Collectors.toList());
      try {
        metricsList.sort(
            (m1, m2) -> (int) (m1.getTimestamp() - m2.getTimestamp()));
        Assert.assertEquals(2, metricsList.size());
        MetricValues metricValues = metricsList.get(0);
        Assert.assertEquals(metricValues.getTags(), finalTags);
        List<MetricValue> values = new ArrayList<>(metricValues.getMetrics());
        Assert.assertEquals(2, values.size());
        values.sort(Comparator.comparing(MetricValue::getName));

        Assert.assertEquals(3, values.get(0).getValue());
        Assert.assertEquals(MetricType.COUNTER, values.get(0).getType());

        Assert.assertEquals(2, values.get(1).getValue());
        Assert.assertEquals(MetricType.GAUGE, values.get(1).getType());

        metricValues = metricsList.get(1);
        values = new ArrayList<>(metricValues.getMetrics());
        Assert.assertEquals(metricValues.getTags(), finalTags);
        Assert.assertEquals(1, values.get(0).getValue());
        Assert.assertEquals(MetricType.COUNTER, values.get(0).getType());
        return true;
      } catch (AssertionError e) {
        return false;
      }
    }, 5, TimeUnit.SECONDS);

    // Writes a program terminate message to unblock stopping of the client service
    programStateWriter.completed(PROGRAM_RUN_ID);
  }

  /**
   * Test for {@link RuntimeClientService} that will terminate itself when
   * seeing program completed message.
   */
  @Test
  public void testProgramTerminate() throws Exception {
    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    runtimeClientService.startAndWait();
    MessagingContext messagingContext = new MultiThreadMessagingContext(
        clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();

    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    // Send a terminate program state first, wait for the service sees the state change,
    // then publish messages to other topics.
    programStateWriter.completed(PROGRAM_RUN_ID);
    Tasks.waitFor(true, () -> runtimeClientService.getProgramFinishTime() >= 0,
        2, TimeUnit.SECONDS);

    for (String topic : nonStatusTopicNames) {
      List<String> payloads = Arrays.asList("msg" + topic, "msg" + topic,
          "msg" + topic);
      messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
          StandardCharsets.UTF_8, payloads.iterator());
    }

    // The client service should get stopped by itself.
    Tasks.waitFor(Service.State.TERMINATED, () -> runtimeClientService.state(),
        clientCConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS)
            + 2000, TimeUnit.MILLISECONDS);

    // All messages should be sent after the runtime client service stopped
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(
        messagingService);
    for (String topic : nonStatusTopicNames) {
      Tasks.waitFor(Arrays.asList("msg" + topic, "msg" + topic, "msg" + topic),
          () -> fetchMessages(serverMessagingContext, topic, 10, null)
              .stream().map(Message::getPayloadAsString)
              .collect(Collectors.toList()),
          5, TimeUnit.SECONDS);
    }
    waitForStatus(serverMessagingContext, ProgramRunStatus.COMPLETED);
  }

  /**
   * Test for {@link RuntimeClientService} that will block termination until a
   * program completed mess
   */
  @Test(timeout = 10000L)
  public void testRuntimeClientStop() throws Exception {
    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    runtimeClientService.startAndWait();
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    ListenableFuture<Service.State> stopFuture = runtimeClientService.stop();
    try {
      stopFuture.get(2, TimeUnit.SECONDS);
      Assert.fail("Expected runtime client service not stopped");
    } catch (TimeoutException e) {
      // Expected
    }

    // Publish a program completed state, which should unblock the client service stop.
    programStateWriter.completed(PROGRAM_RUN_ID);
    stopFuture.get();
  }

  /**
   * Test that runtime can complete even if run was stopped externally
   */
  @Test(timeout = 10000L)
  public void testExternalStop() throws Exception {
    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    runtimeClientService.startAndWait();
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);
    MessagingContext messagingContext = new MultiThreadMessagingContext(
        clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();

    //Validate proper topic order as it's important for not to loose messages
    Assert.assertEquals(
        "Topics must be in proper order to ensure status messages are sent out last",
        topicNames, runtimeClientService.getTopicNames());

    //Mark run as externally stopped
    runCompleted.set(true);

    //Publish a message
    String topic = nonStatusTopicNames.get(0);
    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
        "msg1" + topic, "msg2" + topic);

    ListenableFuture<Service.State> stopFuture = runtimeClientService.stop();
    try {
      stopFuture.get(2, TimeUnit.SECONDS);
      Assert.fail("Expected runtime client service not stopped");
    } catch (TimeoutException e) {
      // Expected
    }

    // Publish a program completed state, which should unblock the client service stop.
    programStateWriter.completed(PROGRAM_RUN_ID);
    stopFuture.get();
  }

  private List<Message> fetchMessages(MessagingContext messagingContext,
      String topic, int limit,
      @Nullable String lastMessageId) {
    try {
      MessageFetcher messageFetcher = messagingContext.getMessageFetcher();
      try (CloseableIterator<Message> iterator = messageFetcher.fetch(
          NamespaceId.SYSTEM.getNamespace(),
          topic, limit, lastMessageId)) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, 0), false)
            .collect(Collectors.toList());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void waitForStatus(MessagingContext serverMessagingContext,
      ProgramRunStatus status)
      throws TimeoutException, InterruptedException, ExecutionException {

    Tasks.waitFor(Collections.singletonList(status),
        () -> topicNames.stream().filter(
                topic -> !nonStatusTopicNames.contains(topic)
            ).flatMap(topic ->
                fetchMessages(serverMessagingContext, topic, 10, null).stream()
                    .map(Message::getPayloadAsString)
                    .map(s -> GSON.fromJson(s, Notification.class))
                    .map(n -> n.getProperties()
                        .get(ProgramOptionConstants.PROGRAM_STATUS))
                    .map(ProgramRunStatus::valueOf))
            .collect(Collectors.toList()), 5, TimeUnit.SECONDS);
  }
}
