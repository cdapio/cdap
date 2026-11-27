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
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.RuntimeMonitor;
import io.cdap.cdap.common.encryption.guice.UserCredentialAeadEncryptionModule;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NoOpAuditLogModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.DatumReader;
import io.cdap.cdap.common.io.DatumWriter;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.Services;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.DatumWriterFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  private AtomicReference<RuntimeRequestValidator> customValidatorRef;
  private Injector clientInjector;

  @Before
  public void beforeTest() throws Exception {
    runCompleted = new AtomicBoolean(false);
    customValidatorRef = new AtomicReference<>();
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
                  RuntimeRequestValidator custom = customValidatorRef.get();
                  if (custom != null) {
                    return custom.validate(programRunId, request);
                  }
                  if (runCompleted.get()) {
                    throw new GoneException();
                  }
                  return ValidatedRuntimeRequest.legacy(
                      new ProgramRunInfo(ProgramRunStatus.RUNNING));
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
        },
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule()
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      Services.startAndWait((Service) messagingService);
    }

    runtimeServer = injector.getInstance(RuntimeServer.class);
    Services.startAndWait(runtimeServer);

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
        new NoOpAuditLogModule(),
        new UserCredentialAeadEncryptionModule(),
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
      Services.startAndWait((Service) clientMessagingService);
    }
    clientProgramStatePublisher = clientInjector.getInstance(
        ProgramStatePublisher.class);
  }

  @After
  public void afterTest() {
    customValidatorRef.set(null);
    runCompleted.set(true);
    if (runtimeClientService != null) {
      try {
        Services.stopAndWait(runtimeClientService);
      } catch (Exception e) {
        // Ignore expected shutdown failures for tests that intentionally fail the service
      }
    }
    runtimeClientService = null;
    if (clientMessagingService instanceof Service) {
      Services.stopAndWait((Service) clientMessagingService);
    }

    Services.stopAndWait(runtimeServer);
    if (messagingService instanceof Service) {
      Services.stopAndWait((Service) messagingService);
    }
  }

  @Test
  public void testBasicRelay() throws Exception {
    runtimeClientService = clientInjector.getInstance(
        RuntimeClientService.class);
    Services.startAndWait(runtimeClientService);
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
    Services.startAndWait(runtimeClientService);
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
    Services.startAndWait(runtimeClientService);
    MessagingContext messagingContext = new MultiThreadMessagingContext(
        clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();

    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    // Send a terminate program state first, wait for the service sees the state change,
    // then publish messages to other topics.
    programStateWriter.completed(PROGRAM_RUN_ID);
    Tasks.waitFor(true, () ->
            runtimeClientService.getProgramCompletionDetails() != null,
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
    Services.startAndWait(runtimeClientService);
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(
        clientProgramStatePublisher);

    ListenableFuture<Service.State> stopFuture = Services.stop(runtimeClientService);
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
    Services.startAndWait(runtimeClientService);
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

    ListenableFuture<Service.State> stopFuture = Services.stop(runtimeClientService);
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

  @Test(timeout = 15000L)
  public void testNormalLoopFailsImmediatelyOnMarkedForbidden() throws Exception {
    for (RuntimeValidationFailure failure : RuntimeValidationFailure.values()) {
      AtomicInteger attempts = new AtomicInteger();
      customValidatorRef.set((runId, req) -> {
        attempts.incrementAndGet();
        throw new RuntimeTokenRefusedException(failure, "refused: " + failure.getReason());
      });

      CConfiguration longRetryConf = CConfiguration.copy(clientCConf);
      configureLongRetryPolicy(longRetryConf);
      longRetryConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);
      longRetryConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 30_000L);

      runtimeClientService = createRuntimeClientService(longRetryConf,
          clientInjector.getInstance(RuntimeClient.class));
      Services.startAndWait(runtimeClientService);

      MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
      messagingContext.getDirectMessagePublisher().publish(
          NamespaceId.SYSTEM.getNamespace(), nonStatusTopicNames.get(0),
          "msg-" + failure.getReason());

      Tasks.waitFor(Service.State.FAILED, () -> runtimeClientService.state(),
          5, TimeUnit.SECONDS);
      Throwable cause = runtimeClientService.getFailureCause();
      Assert.assertTrue("Expected RuntimeTokenRefusedException, got " + cause,
          cause instanceof RuntimeTokenRefusedException);
      Assert.assertEquals(failure.getReason(), ((RuntimeTokenRefusedException) cause).getReason());
      Assert.assertEquals(1, attempts.get());
      runtimeClientService = null;
    }
  }

  @Test(timeout = 10000L)
  public void testProgramStatusHeartbeatRefusalFailsImmediately() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      attempts.incrementAndGet();
      throw new RuntimeTokenRefusedException(
          RuntimeValidationFailure.LEGACY_TOKEN, "heartbeat refused");
    });

    CConfiguration longRetryConf = CConfiguration.copy(clientCConf);
    configureLongRetryPolicy(longRetryConf);
    longRetryConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);
    longRetryConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 60_000L);

    // Non-status topics are empty; only the COMPLETED status event triggers the empty heartbeat.
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(PROGRAM_RUN_ID);

    runtimeClientService = createRuntimeClientService(longRetryConf,
        clientInjector.getInstance(RuntimeClient.class));
    Services.startAndWait(runtimeClientService);

    Tasks.waitFor(Service.State.FAILED, () -> runtimeClientService.state(), 5, TimeUnit.SECONDS);
    Throwable cause = runtimeClientService.getFailureCause();
    Assert.assertTrue("Expected RuntimeTokenRefusedException, got " + cause,
        cause instanceof RuntimeTokenRefusedException);
    Assert.assertEquals(RuntimeValidationFailure.LEGACY_TOKEN.getReason(),
        ((RuntimeTokenRefusedException) cause).getReason());
    Assert.assertEquals(1, attempts.get());
  }

  @Test(timeout = 10000L)
  public void testShutdownUnmarkedForbiddenRetriesAcrossPolicyWindow() throws Exception {
    // Pin baseline shutdown retry behavior for unmarked 403 using a short bounded retry policy.
    AtomicBoolean rejectUnmarked = new AtomicBoolean(false);
    AtomicInteger rejectedAttempts = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      if (rejectUnmarked.get()) {
        rejectedAttempts.incrementAndGet();
        throw new UnauthorizedException("unmarked forbidden");
      }
      return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    });

    CConfiguration shortRetryConf = CConfiguration.copy(clientCConf);
    shortRetryConf.set(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.TYPE,
        "fixed.delay");
    shortRetryConf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_BASE_MS, 20L);
    shortRetryConf.setInt(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_RETRIES, 3);
    shortRetryConf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_TIME_SECS, 5L);
    shortRetryConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 10);
    shortRetryConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 60_000L);
    shortRetryConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 60_000L);

    CountDownLatch statusHeartbeatDone = new CountDownLatch(1);
    // Publish COMPLETED before starting so the initial poll immediately buffers it without
    // waiting for the long poll interval.
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(PROGRAM_RUN_ID);

    runtimeClientService = createRuntimeClientService(shortRetryConf,
        createHeartbeatTrackingClient(shortRetryConf, statusHeartbeatDone));
    Services.startAndWait(runtimeClientService);
    Assert.assertTrue(statusHeartbeatDone.await(5, TimeUnit.SECONDS));
    Assert.assertNotNull(runtimeClientService.getProgramCompletionDetails());

    // Switch to unmarked 403 and publish an unsent non-status message before shutting down.
    rejectUnmarked.set(true);
    MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
    messagingContext.getDirectMessagePublisher().publish(
        NamespaceId.SYSTEM.getNamespace(), nonStatusTopicNames.get(0), "pending-shutdown-msg");

    Services.stopAndWait(runtimeClientService);
    Assert.assertEquals(Service.State.TERMINATED, runtimeClientService.state());
    // 1 initial attempt + 3 retries in prepareClose() and again in close() (>= 4 attempts).
    Assert.assertTrue("Expected multiple shutdown retry attempts for unmarked 403, got "
        + rejectedAttempts.get(), rejectedAttempts.get() >= 4);
  }

  @Test(timeout = 10000L)
  public void testShutdownAbortsImmediatelyOnMarkedForbiddenWithPendingTopicsAndStatus()
      throws Exception {
    AtomicBoolean refuse = new AtomicBoolean(false);
    AtomicInteger refusedAttempts = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      if (refuse.get()) {
        refusedAttempts.incrementAndGet();
        throw new RuntimeTokenRefusedException(
            RuntimeValidationFailure.URL_MISMATCH, "token run mismatch");
      }
      return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    });

    CConfiguration longRetryConf = CConfiguration.copy(clientCConf);
    configureLongRetryPolicy(longRetryConf);
    // Long poll delay so newly published messages are flushed by doShutdown(), not runTask().
    longRetryConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 10);
    longRetryConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 60_000L);
    longRetryConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 60_000L);

    CountDownLatch statusHeartbeatDone = new CountDownLatch(1);
    // Publish COMPLETED before starting so the initial poll buffers lastProgramStateMessages
    // immediately without waiting for the long poll interval.
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(PROGRAM_RUN_ID);

    runtimeClientService = createRuntimeClientService(longRetryConf,
        createHeartbeatTrackingClient(longRetryConf, statusHeartbeatDone));
    Services.startAndWait(runtimeClientService);
    Assert.assertTrue(statusHeartbeatDone.await(5, TimeUnit.SECONDS));
    Assert.assertNotNull(runtimeClientService.getProgramCompletionDetails());

    // Publish unsent messages across multiple non-status topics, then enable marked 403 refusal.
    MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
    for (String topic : nonStatusTopicNames) {
      messagingContext.getDirectMessagePublisher().publish(
          NamespaceId.SYSTEM.getNamespace(), topic, "unsent-" + topic);
    }
    refuse.set(true);

    ListenableFuture<Service.State> stopFuture = Services.stop(runtimeClientService);
    ExecutionException thrown = Assert.assertThrows(
        ExecutionException.class, () -> stopFuture.get(5, TimeUnit.SECONDS));
    Assert.assertTrue("Expected RuntimeTokenRefusedException, got " + thrown.getCause(),
        thrown.getCause() instanceof RuntimeTokenRefusedException);
    Assert.assertEquals(RuntimeValidationFailure.URL_MISMATCH.getReason(),
        ((RuntimeTokenRefusedException) thrown.getCause()).getReason());
    Assert.assertSame(thrown.getCause(), runtimeClientService.getFailureCause());
    Assert.assertEquals(Service.State.FAILED, runtimeClientService.state());
    // Must abort on the very first marked 403 without retrying or flushing remaining topics/status.
    Assert.assertEquals(1, refusedAttempts.get());
  }

  @Test(timeout = 10000L)
  public void testShutdownAbortsImmediatelyOnMarkedForbiddenDuringFinalStatusSend()
      throws Exception {
    String statusTopicPrefix = clientCConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    AtomicBoolean refuseStatus = new AtomicBoolean(false);
    AtomicInteger refusedStatusAttempts = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      if (refuseStatus.get() && req.uri().contains("/topics/" + statusTopicPrefix)) {
        refusedStatusAttempts.incrementAndGet();
        throw new RuntimeTokenRefusedException(
            RuntimeValidationFailure.MISSING_SCOPES, "missing runtime scopes");
      }
      return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    });

    CConfiguration longRetryConf = CConfiguration.copy(clientCConf);
    configureLongRetryPolicy(longRetryConf);
    longRetryConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 10);
    longRetryConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 60_000L);
    longRetryConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 60_000L);

    CountDownLatch statusHeartbeatDone = new CountDownLatch(1);
    // Publish COMPLETED before starting so the initial poll buffers lastProgramStateMessages.
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(PROGRAM_RUN_ID);

    runtimeClientService = createRuntimeClientService(longRetryConf,
        createHeartbeatTrackingClient(longRetryConf, statusHeartbeatDone));
    Services.startAndWait(runtimeClientService);
    Assert.assertTrue(statusHeartbeatDone.await(5, TimeUnit.SECONDS));
    Assert.assertNotNull(runtimeClientService.getProgramCompletionDetails());

    // Non-status topics are empty; only the final lastProgramStateMessages flush hits the server.
    refuseStatus.set(true);
    ListenableFuture<Service.State> stopFuture = Services.stop(runtimeClientService);
    ExecutionException thrown = Assert.assertThrows(
        ExecutionException.class, () -> stopFuture.get(5, TimeUnit.SECONDS));
    Assert.assertTrue("Expected RuntimeTokenRefusedException, got " + thrown.getCause(),
        thrown.getCause() instanceof RuntimeTokenRefusedException);
    Assert.assertEquals(RuntimeValidationFailure.MISSING_SCOPES.getReason(),
        ((RuntimeTokenRefusedException) thrown.getCause()).getReason());
    Assert.assertSame(thrown.getCause(), runtimeClientService.getFailureCause());
    Assert.assertEquals(Service.State.FAILED, runtimeClientService.state());
    Assert.assertEquals(1, refusedStatusAttempts.get());
  }

  @Test(timeout = 15000L)
  public void testUnmarkedForbiddenUnauthorizedAndServerErrorContinueRetrying() throws Exception {
    List<Exception> transientErrors = Arrays.asList(
        new UnauthorizedException("unmarked forbidden"),
        new UnauthenticatedException("unauthenticated"),
        new RuntimeException("internal server error"));
    CConfiguration fastRetryConf = createFastRetryConf();
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(messagingService);
    MessagingContext clientMessaging = new MultiThreadMessagingContext(clientMessagingService);
    String topic = nonStatusTopicNames.get(0);

    for (int i = 0; i < transientErrors.size(); i++) {
      ProgramRunId subRunId = NamespaceId.DEFAULT.app("app").workflow("wf")
          .run(RunIds.generate());
      Exception err = transientErrors.get(i);
      AtomicInteger failuresLeft = new AtomicInteger(2);
      AtomicInteger attempts = new AtomicInteger();
      customValidatorRef.set((runId, req) -> {
        if (!subRunId.equals(runId)) {
          return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
        }
        attempts.incrementAndGet();
        if (failuresLeft.getAndDecrement() > 0) {
          if (err instanceof RuntimeException) {
            throw (RuntimeException) err;
          }
          throw (AccessException) err;
        }
        return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
      });

      String payload = "recovered-" + i;
      clientMessaging.getDirectMessagePublisher().publish(
          NamespaceId.SYSTEM.getNamespace(), topic, payload);

      runtimeClientService = createRuntimeClientService(fastRetryConf,
          clientInjector.getInstance(RuntimeClient.class), subRunId);
      Services.startAndWait(runtimeClientService);

      Tasks.waitFor(true, () -> fetchMessages(serverMessagingContext, topic, 50, null).stream()
              .map(Message::getPayloadAsString)
              .anyMatch(payload::equals),
          5, TimeUnit.SECONDS);
      Assert.assertTrue("Expected at least 3 attempts (2 failures + 1 success)",
          attempts.get() >= 3);
      Assert.assertEquals(Service.State.RUNNING, runtimeClientService.state());

      new MessagingProgramStateWriter(clientProgramStatePublisher).completed(subRunId);
      Services.stopAndWait(runtimeClientService);
      runtimeClientService = null;
    }
  }

  @Test(timeout = 10000L)
  public void testMixedVersionsNewServerWithLegacyClientAndOldServerWithNewClient()
      throws Exception {
    CConfiguration fastRetryConf = createFastRetryConf();

    // (a) New server emits marked 403 + header; legacy client ignores header (throws IOException)
    //     and retries until server recovers.
    ProgramRunId legacyRunId = NamespaceId.DEFAULT.app("app").workflow("wf")
        .run(RunIds.generate());
    AtomicInteger markedFailuresLeft = new AtomicInteger(2);
    AtomicInteger legacyClientAttempts = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      legacyClientAttempts.incrementAndGet();
      if (markedFailuresLeft.getAndDecrement() > 0) {
        throw new RuntimeTokenRefusedException(
            RuntimeValidationFailure.LEGACY_TOKEN, "legacy token refused");
      }
      return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    });
    RuntimeClient legacyClient = new RuntimeClient(
        fastRetryConf, clientInjector.getInstance(RemoteClientFactory.class)) {
      @Override
      public void sendMessages(ProgramRunId runId, TopicId topicId, Iterator<Message> messages)
          throws IOException, BadRequestException, GoneException, NotFoundException {
        try {
          super.sendMessages(runId, topicId, messages);
        } catch (RuntimeTokenRefusedException ignoredHeader) {
          // Simulate pre-change RuntimeClient that ignores X-CDAP-Runtime-Token-Failure on 403.
          throw new IOException("HTTP 403 (legacy client ignores failure header)", ignoredHeader);
        }
      }
    };
    MessagingContext clientMessaging = new MultiThreadMessagingContext(clientMessagingService);
    String topic = nonStatusTopicNames.get(0);
    clientMessaging.getDirectMessagePublisher().publish(
        NamespaceId.SYSTEM.getNamespace(), topic, "mixed-legacy-client");
    runtimeClientService = createRuntimeClientService(fastRetryConf, legacyClient, legacyRunId);
    Services.startAndWait(runtimeClientService);
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(messagingService);
    Tasks.waitFor(true, () -> fetchMessages(serverMessagingContext, topic, 50, null).stream()
            .map(Message::getPayloadAsString)
            .anyMatch("mixed-legacy-client"::equals),
        5, TimeUnit.SECONDS);
    Assert.assertTrue("Expected at least 3 attempts, got " + legacyClientAttempts.get(),
        legacyClientAttempts.get() >= 3);
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(legacyRunId);
    Services.stopAndWait(runtimeClientService);
    runtimeClientService = null;

    // (b) Old server emits unmarked 403 then unmarked 500; new RuntimeClientService retries.
    ProgramRunId oldServerRunId = NamespaceId.DEFAULT.app("app").workflow("wf")
        .run(RunIds.generate());
    AtomicInteger oldServerAttempt = new AtomicInteger();
    customValidatorRef.set((runId, req) -> {
      int n = oldServerAttempt.incrementAndGet();
      if (n == 1) {
        throw new UnauthorizedException("old server unmarked 403");
      }
      if (n == 2) {
        throw new RuntimeException("old server unmarked 500");
      }
      return ValidatedRuntimeRequest.legacy(new ProgramRunInfo(ProgramRunStatus.RUNNING));
    });
    clientMessaging.getDirectMessagePublisher().publish(
        NamespaceId.SYSTEM.getNamespace(), topic, "mixed-old-server");
    runtimeClientService = createRuntimeClientService(fastRetryConf,
        clientInjector.getInstance(RuntimeClient.class), oldServerRunId);
    Services.startAndWait(runtimeClientService);
    Tasks.waitFor(true, () -> fetchMessages(serverMessagingContext, topic, 50, null).stream()
            .map(Message::getPayloadAsString)
            .anyMatch("mixed-old-server"::equals),
        5, TimeUnit.SECONDS);
    Assert.assertTrue("Expected at least 3 attempts, got " + oldServerAttempt.get(),
        oldServerAttempt.get() >= 3);
    new MessagingProgramStateWriter(clientProgramStatePublisher).completed(oldServerRunId);
    Services.stopAndWait(runtimeClientService);
    runtimeClientService = null;
  }

  private CConfiguration createFastRetryConf() {
    CConfiguration conf = CConfiguration.copy(clientCConf);
    conf.set(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.TYPE, "fixed.delay");
    conf.setLong(
        Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_BASE_MS, 20L);
    conf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 50L);
    conf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 60_000L);
    return conf;
  }

  private RuntimeClient createHeartbeatTrackingClient(CConfiguration conf,
      CountDownLatch statusHeartbeatDone) {
    String statusTopicPrefix = conf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC);
    return new RuntimeClient(conf, clientInjector.getInstance(RemoteClientFactory.class)) {
      @Override
      public void sendMessages(ProgramRunId runId, TopicId topicId, Iterator<Message> messages)
          throws IOException, BadRequestException, GoneException, NotFoundException,
          RuntimeTokenRefusedException {
        try {
          super.sendMessages(runId, topicId, messages);
        } finally {
          if (topicId.getTopic().startsWith(statusTopicPrefix)) {
            statusHeartbeatDone.countDown();
          }
        }
      }
    };
  }

  private RuntimeClientService createRuntimeClientService(CConfiguration conf,
      RuntimeClient client) {
    return createRuntimeClientService(conf, client, PROGRAM_RUN_ID);
  }

  private RuntimeClientService createRuntimeClientService(CConfiguration conf,
      RuntimeClient client, ProgramRunId runId) {
    return new RuntimeClientService(
        conf,
        clientMessagingService,
        client,
        runId,
        clientInjector.getInstance(SchemaGenerator.class),
        clientInjector.getInstance(DatumWriterFactory.class),
        clientInjector.getInstance(DatumReaderFactory.class));
  }

  private static void configureLongRetryPolicy(CConfiguration conf) {
    conf.set(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.TYPE,
        "exponential.backoff");
    conf.setLong(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_BASE_MS,
        50L);
    conf.setLong(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.DELAY_MAX_MS,
        1_000L);
    conf.setInt(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_RETRIES,
        Integer.MAX_VALUE);
    conf.setLong(Constants.Service.RUNTIME_MONITOR_RETRY_PREFIX + Constants.Retry.MAX_TIME_SECS,
        3_600L);
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
