/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.events;

import com.google.gson.JsonObject;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.events.dummy.DummyEventWriter;
import io.cdap.cdap.internal.events.dummy.DummyEventWriterExtensionProvider;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventWriter;
import io.cdap.cdap.spi.events.EventWriterContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the {@link ProgramStatusEventPublisher}.
 */
public class ProgramStatusEventPublisherTest extends AppFabricTestBase {
  private static final String MOCKED_NOTIFICATION_FILENAME = "mocked_pipeline_notification.json";
  private static final Logger LOGGER = LoggerFactory.getLogger(ProgramStatusEventPublisherTest.class);

  private static EventPublisher eventPublisher;

  @BeforeClass
  public static void setupClass() throws IOException {
    eventPublisher = getInjector().getInstance(ProgramStatusEventPublisher.class);
  }

  @AfterClass
  public static void tearDown() {
    eventPublisher.stopPublish();
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testInitialize() {
    EventWriterProvider provider = new DummyEventWriterExtensionProvider(new DummyEventWriter());
    Map<String, EventWriter> eventWriterMap = provider.loadEventWriters();
    try {
      eventPublisher.initialize(eventWriterMap.values());
      eventPublisher.startPublish();
    } catch (Exception ex) {
      LOGGER.error("Error during Event Publisher initialization.", ex);
      Assert.fail("Error while initializing Event Publisher");
    }
  }

  @Test
  public void testMessageWorkflow() {
    ProgramStatusEventPublisher programStatusEventPublisher = (ProgramStatusEventPublisher) eventPublisher;
    try {
      programStatusEventPublisher.processMessages(null, provideMockedMessages());
    } catch (Exception e) {
      LOGGER.error("Error during message process.", e);
      Assert.fail("Error during message process");
    }

  }

  @Test
  public void testRuntimeScopesStrippedFromSystemArgs() {
    ProgramStatusEventPublisher publisher =
        getInjector().getInstance(ProgramStatusEventPublisher.class);
    List<Event<?>> captured = new ArrayList<>();
    EventWriter capturingWriter = new EventWriter() {
      @Override
      public String getID() {
        return "capturing-test-writer";
      }

      @Override
      public void initialize(EventWriterContext eventWriterContext) {
      }

      @Override
      public void write(Collection<? extends Event<?>> events) {
        captured.addAll(events);
      }

      @Override
      public void close() {
      }
    };
    publisher.initialize(Collections.singletonList(capturingWriter));

    ProgramRunId runId = NamespaceId.DEFAULT.app("app1").workflow("wf1").run(RunIds.generate());
    Map<String, String> systemOverrides = new HashMap<>();
    systemOverrides.put(ProgramOptionConstants.RUNTIME_SCOPES, "{\"connections\":[\"c1\"]}");
    systemOverrides.put("other.key", "other-value");
    Map<String, String> propsWithOverrides = new HashMap<>();
    propsWithOverrides.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    propsWithOverrides.put(ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.STARTING.name());
    propsWithOverrides.put(ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(systemOverrides));
    Notification withOverrides =
        new Notification(Notification.Type.PROGRAM_STATUS, propsWithOverrides);
    publisher.processMessages(null,
        Collections.singletonList(new ImmutablePair<>("msg-1", withOverrides)).iterator());

    Assert.assertEquals(1, captured.size());
    JsonObject detailsJson = GSON.toJsonTree(captured.get(0).getEventDetails()).getAsJsonObject();
    JsonObject systemArgs = detailsJson.getAsJsonObject("systemArgs");
    Assert.assertNotNull(systemArgs);
    Assert.assertEquals("other-value", systemArgs.get("other.key").getAsString());
    Assert.assertFalse(systemArgs.has(ProgramOptionConstants.RUNTIME_SCOPES));

    Map<String, String> propsWithoutOverrides = new HashMap<>();
    propsWithoutOverrides.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId));
    propsWithoutOverrides.put(ProgramOptionConstants.PROGRAM_STATUS,
        ProgramRunStatus.STARTING.name());
    Notification withoutOverrides =
        new Notification(Notification.Type.PROGRAM_STATUS, propsWithoutOverrides);
    publisher.processMessages(null,
        Collections.singletonList(new ImmutablePair<>("msg-2", withoutOverrides)).iterator());

    Assert.assertEquals(2, captured.size());
  }

  private Iterator<ImmutablePair<String, Notification>> provideMockedMessages() {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream notificationIS = classLoader.getResourceAsStream(MOCKED_NOTIFICATION_FILENAME);
    Assert.assertNotNull(notificationIS);
    String notificationJson = new BufferedReader(new InputStreamReader(notificationIS))
      .lines().collect(Collectors.joining(System.lineSeparator()));
    Notification notification = GSON.fromJson(notificationJson, Notification.class);
    ImmutablePair<String, Notification> message = new ImmutablePair<>("test", notification);
    List<ImmutablePair<String, Notification>> messageList = new ArrayList<>();
    messageList.add(message);
    return messageList.iterator();
  }
}
