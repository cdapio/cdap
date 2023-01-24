/*
 * Copyright © 2020-2022 Cask Data, Inc.
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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.program.MessagingProgramStatePublisher;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.ProgramStatePublisher;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Unit test for {@link RuntimeClientService}.
 */
public class RuntimeClientServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int PROGRAM_STATUS_EVENT_TEST_PARTITIONS = 10;

  // The cConf value for the runtime monitor topic configs to have two topics, and one has to be program status event
  // This is for testing the RuntimeClientService handling of program status correctly
  private static final String TOPIC_CONFIGS_VALUE =
    Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC
      + "," + Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC + ":" + PROGRAM_STATUS_EVENT_TEST_PARTITIONS
      + "," + Constants.Metadata.MESSAGING_TOPIC
      + "," + Constants.Audit.TOPIC;

  private static final ProgramRunId PROGRAM_RUN_ID =
    NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
  private static final Gson GSON = new Gson();

  private Map<String, String> topicConfigs;

  // Services for the runtime server side
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;

  // Services for the runtime client side
  private CConfiguration clientCConf;
  private MessagingService clientMessagingService;
  private RuntimeClientService runtimeClientService;
  private ProgramStatePublisher clientProgramStatePublisher;

  private AtomicBoolean runCompleted = new AtomicBoolean(false);

  @Before
  public void beforeTest() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, TOPIC_CONFIGS_VALUE);
    cConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, PROGRAM_STATUS_EVENT_TEST_PARTITIONS);

    topicConfigs = RuntimeMonitors.createTopicConfigs(cConf);

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
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> { });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
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
    clientCConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    clientCConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, TOPIC_CONFIGS_VALUE);
    clientCConf.setInt(Constants.AppFabric.PROGRAM_STATUS_EVENT_NUM_PARTITIONS, PROGRAM_STATUS_EVENT_TEST_PARTITIONS);

    // Shorten the poll delay and grace period to speed up testing of program terminate state handling
    clientCConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 200);
    clientCConf.setLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS, 3000);
    // Use smaller batch size so that fetches is broken into multiple fetches
    clientCConf.setInt(Constants.RuntimeMonitor.BATCH_SIZE, 1);

    injector = Guice.createInjector(
      new ConfigModule(clientCConf),
      RemoteAuthenticatorModules.getNoOpModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ProgramStatePublisher.class).to(MessagingProgramStatePublisher.class);
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(DiscoveryService.class).toInstance(discoveryService);
          bind(DiscoveryServiceClient.class).toInstance(discoveryService);
          bind(ProgramRunId.class).toInstance(PROGRAM_RUN_ID);
        }
      }
    );

    clientMessagingService = injector.getInstance(MessagingService.class);
    if (clientMessagingService instanceof Service) {
      ((Service) clientMessagingService).startAndWait();
    }
    clientProgramStatePublisher = injector.getInstance(ProgramStatePublisher.class);
    runtimeClientService = injector.getInstance(RuntimeClientService.class);
    runtimeClientService.startAndWait();
  }

  @After
  public void afterTest() {
    runtimeClientService.stopAndWait();
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
    // Send some messages to multiple topics in the client side TMS, they should get replicated to the server side TMS.
    MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(clientProgramStatePublisher);

    // For program status event topic, we need to send valid program status event because
    // the RuntimeClientService will decode it to watch for program termination
    programStateWriter.running(PROGRAM_RUN_ID, null);

    for (Map.Entry<String, String> entry : topicConfigs.entrySet()) {
      if (!entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), entry.getValue(), entry.getKey(), entry.getKey());
      }
    }

    // Extract the program run status from the Notification
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(messagingService);
    waitForStatus(serverMessagingContext, ProgramRunStatus.RUNNING);

    for (Map.Entry<String, String> entry : topicConfigs.entrySet()) {
      if (!entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        Tasks.waitFor(Arrays.asList(entry.getKey(), entry.getKey()),
                      () -> fetchMessages(serverMessagingContext, entry.getValue(), 10, null)
                        .stream().map(Message::getPayloadAsString).collect(Collectors.toList()),
                      5, TimeUnit.SECONDS);
      }
    }

    for (Map.Entry<String, String> entry : topicConfigs.entrySet()) {
      if (!entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        Tasks.waitFor(Arrays.asList(entry.getKey(), entry.getKey()),
                      () -> fetchMessages(serverMessagingContext, entry.getValue(), 10, null)
                        .stream().map(Message::getPayloadAsString).collect(Collectors.toList()),
                      5, TimeUnit.SECONDS);
      }
    }

    // Writes a program terminate message to unblock stopping of the client service
    programStateWriter.completed(PROGRAM_RUN_ID);
  }

  /**
   * Test for {@link RuntimeClientService} that will terminate itself when seeing program completed message.
   */
  @Test
  public void testProgramTerminate() throws Exception {
    MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();

    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(clientProgramStatePublisher);

    // Send a terminate program state first, wait for the service sees the state change,
    // then publish messages to other topics.
    programStateWriter.completed(PROGRAM_RUN_ID);
    Tasks.waitFor(true, () -> runtimeClientService.getProgramFinishTime() >= 0, 2, TimeUnit.SECONDS);

    for (Map.Entry<String, String> entry : topicConfigs.entrySet()) {
      if (!entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        List<String> payloads = Arrays.asList(entry.getKey(), entry.getKey(), entry.getKey());
        messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), entry.getValue(),
                                 StandardCharsets.UTF_8, payloads.iterator());
      }
    }

    // The client service should get stopped by itself.
    Tasks.waitFor(Service.State.TERMINATED, () -> runtimeClientService.state(),
                  clientCConf.getLong(Constants.RuntimeMonitor.GRACEFUL_SHUTDOWN_MS) + 2000, TimeUnit.MILLISECONDS);

    // All messages should be sent after the runtime client service stopped
    MessagingContext serverMessagingContext = new MultiThreadMessagingContext(messagingService);
    for (Map.Entry<String, String> entry : topicConfigs.entrySet()) {
      if (!entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)) {
        Tasks.waitFor(Arrays.asList(entry.getKey(), entry.getKey(), entry.getKey()),
                      () -> fetchMessages(serverMessagingContext, entry.getValue(), 10, null)
                        .stream().map(Message::getPayloadAsString).collect(Collectors.toList()),
                      5, TimeUnit.SECONDS);
      }
    }
    waitForStatus(serverMessagingContext, ProgramRunStatus.COMPLETED);
  }

  /**
   * Test for {@link RuntimeClientService} that will block termination until a program completed mess
   */
  @Test(timeout = 10000L)
  public void testRuntimeClientStop() throws Exception {
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(clientProgramStatePublisher);

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
    ProgramStateWriter programStateWriter = new MessagingProgramStateWriter(clientProgramStatePublisher);
    MessagingContext messagingContext = new MultiThreadMessagingContext(clientMessagingService);
    MessagePublisher messagePublisher = messagingContext.getDirectMessagePublisher();

    //Validate proper topic order as it's important for not to loose messages
    Assert.assertEquals(
      "Topics must be in proper order to ensure status messages are sent out last",
      new ArrayList<>(topicConfigs.keySet()), new ArrayList<>(runtimeClientService.getTopicNames()));

    //Mark run as externally stopped
    runCompleted.set(true);

    //Publish a message
    Map.Entry<String, String> topicEntry = topicConfigs.entrySet().iterator().next();
    Assert.assertFalse(
      "Expected topics to be sorted so that status ones are last ones",
      topicEntry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));

    messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topicEntry.getValue(),
                             topicEntry.getKey(), topicEntry.getKey());

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

  private List<Message> fetchMessages(MessagingContext messagingContext, String topic, int limit,
                                      @Nullable String lastMessageId) {
    try {
      MessageFetcher messageFetcher = messagingContext.getMessageFetcher();
      try (CloseableIterator<Message> iterator = messageFetcher.fetch(NamespaceId.SYSTEM.getNamespace(),
                                                                      topic, limit, lastMessageId)) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
          .collect(Collectors.toList());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void waitForStatus(MessagingContext serverMessagingContext, ProgramRunStatus status)
    throws TimeoutException, InterruptedException, ExecutionException {

    Tasks.waitFor(Collections.singletonList(status),
                  () -> topicConfigs.entrySet().stream().filter(
                      entry -> entry.getKey().startsWith(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC)
                    ).flatMap(entry ->
                                fetchMessages(serverMessagingContext, entry.getValue(), 10, null).stream()
                                  .map(Message::getPayloadAsString)
                                  .map(s -> GSON.fromJson(s, Notification.class))
                                  .map(n -> n.getProperties().get(ProgramOptionConstants.PROGRAM_STATUS))
                                  .map(ProgramRunStatus::valueOf))
                    .collect(Collectors.toList()), 5, TimeUnit.SECONDS);
  }
}
