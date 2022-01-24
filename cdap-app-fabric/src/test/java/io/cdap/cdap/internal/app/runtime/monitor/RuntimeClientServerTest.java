/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.RuntimeServerModule;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Unit test for {@link RuntimeServer} and {@link RuntimeClient}.
 */
@RunWith(Parameterized.class)
public class RuntimeClientServerTest {

  public static final String TEST_TOPIC_KEY = "topic.key";
  public static final String TEST_TOPIC = "topic";

  @Parameterized.Parameters(name = "{index}: compression = {0}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {false},
      {true},
    });
  }

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final List<String> logEntries = new ArrayList<>();
  private final boolean compression;
  private CConfiguration cConf;
  private MessagingService messagingService;
  private RuntimeServer runtimeServer;
  private RuntimeClient runtimeClient;
  private LocationFactory locationFactory;

  public RuntimeClientServerTest(boolean compression) {
    this.compression = compression;
  }

  @Before
  public void beforeTest() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.RuntimeMonitor.COMPRESSION_ENABLED, compression);
    cConf.setBoolean(Constants.AppFabric.SPARK_EVENT_LOGS_ENABLED, true);
    cConf.set(TEST_TOPIC_KEY, TEST_TOPIC);
    cConf.set(Constants.RuntimeMonitor.TOPICS_CONFIGS, Constants.Logging.TMS_TOPIC_PREFIX + ":1," + TEST_TOPIC_KEY);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new LocalLocationModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new RuntimeServerModule() {
        @Override
        protected void bindRequestValidator() {
          bind(RuntimeRequestValidator.class).toInstance(
            (programRunId, request) -> new ProgramRunInfo(ProgramRunStatus.COMPLETED, null));
        }

        @Override
        protected void bindLogProcessor() {
          bind(RemoteExecutionLogProcessor.class).toInstance(payloads -> {
            // For testing purpose, we just store logs to a list
            payloads.forEachRemaining(bytes -> logEntries.add(new String(bytes, StandardCharsets.UTF_8)));
          });
        }
      },
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    messagingService.createTopic(new TopicMetadata(NamespaceId.SYSTEM.topic("topic")));

    runtimeServer = injector.getInstance(RuntimeServer.class);
    runtimeServer.startAndWait();

    runtimeClient = injector.getInstance(RuntimeClient.class);
    locationFactory = injector.getInstance(LocationFactory.class);
  }

  @After
  public void afterTest() {
    logEntries.clear();
    runtimeServer.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testSmallMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    List<Message> messages = new ArrayList<>();
    messages.add(createMessage(Math.max(1, RuntimeClient.CHUNK_SIZE / 4)));

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testLargeMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic(TEST_TOPIC);

    List<Message> messages = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(RuntimeClient.CHUNK_SIZE * 2));
    }

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testMixedSendMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic("topic");

    List<Message> messages = new ArrayList<>();

    // Generate a mix of large and small messages
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(i + 1));
    }
    for (int i = 0; i < 10; i++) {
      messages.add(createMessage(i + RuntimeClient.CHUNK_SIZE));
    }

    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());
    assertMessages(topicId, messages);
  }

  @Test
  public void testLogMessage() throws Exception {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());
    TopicId topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.Logging.TMS_TOPIC_PREFIX) + "0");

    List<Message> messages = IntStream.range(0, 100).mapToObj(this::createMessage).collect(Collectors.toList());
    runtimeClient.sendMessages(programRunId, topicId, messages.iterator());

    List<String> expected = messages.stream().map(Message::getPayloadAsString).collect(Collectors.toList());
    Assert.assertEquals(expected, logEntries);
  }

  @Test (timeout = 2000L)
  public void testSparkEventLogs() throws Exception {
    File eventFile = TEMP_FOLDER.newFile();
    Files.write(eventFile.toPath(), "Testing".getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);

    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());
    runtimeClient.uploadSparkEventLogs(programRunId, eventFile);

    // Verify by reading from the event logs location
    List<Location> eventFiles = locationFactory.create(cConf.get(Constants.AppFabric.SPARK_EVENT_LOGS_DIR)).list()
      .stream()
      .filter(l -> l.getName().endsWith(eventFile.getName()))
      .collect(Collectors.toList());

    Assert.assertEquals(1, eventFiles.size());
    try (InputStream is = eventFiles.get(0).getInputStream()) {
      Assert.assertEquals("Testing", new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }
  }

  private void assertMessages(TopicId topicId, Collection<Message> messages) throws Exception {
    // Read the messages from TMS and compare
    MessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    List<Message> fetchedMessages = new ArrayList<>();
    try (CloseableIterator<Message> iterator =
           messagingContext.getMessageFetcher()
             .fetch(topicId.getNamespace(), topicId.getTopic(), Integer.MAX_VALUE, null)) {
      iterator.forEachRemaining(fetchedMessages::add);
    }

    Assert.assertEquals(
      messages.stream().map(Message::getPayloadAsString).collect(Collectors.toList()),
      fetchedMessages.stream().map(Message::getPayloadAsString).collect(Collectors.toList())
    );
  }

  /**
   * Creates a {@link Message} with a payload of the given size.
   */
  private Message createMessage(int size) {
    String messageId = RunIds.generate().getId();
    byte[] payload = Strings.repeat("m", size).getBytes(StandardCharsets.UTF_8);

    return new Message() {
      @Override
      public String getId() {
        return messageId;
      }

      @Override
      public byte[] getPayload() {
        return payload;
      }
    };
  }
}
