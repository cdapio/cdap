/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.events.dummy.DummyEventWriter;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.events.Event;
import io.cdap.cdap.spi.events.EventWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link ProgramStatusEventPublisher} lifecycle without {@code AppFabricTestBase}.
 */
public class ProgramStatusEventPublisherUnitTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testStartPublishWaitsForRunningAndStopPublishStopsService() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.Event.PROGRAM_STATUS_POLL_INTERVAL_SECONDS, 1);

    Collection<Event<?>> writtenEvents = new CopyOnWriteArrayList<>();
    EventWriter capturingWriter = new DummyEventWriter() {
      @Override
      public void write(Collection<? extends Event<?>> events) {
        writtenEvents.addAll(events);
      }
    };

    ProgramStatusEventPublisher publisher = new ProgramStatusEventPublisher(
        cConf,
        Mockito.mock(MessagingService.class),
        new NoOpMetricsCollectionService(),
        Mockito.mock(TransactionRunner.class),
        Mockito.mock(MetricsProvider.class));

    publisher.initialize(Collections.singletonList(capturingWriter));
    publisher.startPublish();
    Assert.assertEquals(Service.State.RUNNING, publisher.state());

    ProgramRunId runId = NamespaceId.DEFAULT.app("myApp").workflow("myWf").run(RunIds.generate());
    Notification notification = new Notification(
        Notification.Type.PROGRAM_STATUS,
        ImmutableMap.of(
            ProgramOptionConstants.PROGRAM_STATUS, ProgramRunStatus.STARTING.name(),
            ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(runId),
            ProgramOptionConstants.USER_OVERRIDES, "{}",
            ProgramOptionConstants.SYSTEM_OVERRIDES, "{}"));
    publisher.processMessages(
        null, Collections.singletonList(ImmutablePair.of("m1", notification)).iterator());
    Assert.assertEquals(1, writtenEvents.size());

    publisher.stopPublish();
    long deadline = System.currentTimeMillis() + 5000L;
    while (publisher.state() != Service.State.TERMINATED && System.currentTimeMillis() < deadline) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    Assert.assertEquals(Service.State.TERMINATED, publisher.state());
  }
}
