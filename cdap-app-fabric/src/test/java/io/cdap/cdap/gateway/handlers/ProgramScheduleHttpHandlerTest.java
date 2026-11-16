/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.gateway.handlers.util.ProgramHandlerUtil;
import io.cdap.cdap.internal.MockResponder;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ProgramScheduleHttpHandler}.
 */
public class ProgramScheduleHttpHandlerTest {

  @Test
  public void testAddScheduleAppliesDefaultsWhenOptionalFieldsAreNull() throws Exception {
    ProgramScheduleService scheduleService = Mockito.mock(ProgramScheduleService.class);
    ProgramLifecycleService lifecycleService = Mockito.mock(ProgramLifecycleService.class);
    Store store = Mockito.mock(Store.class);

    ProgramScheduleHttpHandler handler =
        new ProgramScheduleHttpHandler(scheduleService, lifecycleService, store);

    ScheduleDetail minimalDetail = new ScheduleDetail(
        "default",
        "myApp",
        "mySched",
        null,
        new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "myWorkflow"),
        null,
        new ProtoTrigger.TimeTrigger("0 * * * *"),
        null,
        null,
        null,
        null);

    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.PUT,
        "/v3/namespaces/default/apps/myApp/schedules/mySched",
        Unpooled.copiedBuffer(
            ProgramHandlerUtil.toJson(minimalDetail), StandardCharsets.UTF_8));
    MockResponder responder = new MockResponder();

    handler.addSchedule(request, responder, "default", "myApp", "mySched");
    Assert.assertEquals(HttpResponseStatus.OK, responder.getStatus());

    ArgumentCaptor<ProgramSchedule> captor = ArgumentCaptor.forClass(ProgramSchedule.class);
    Mockito.verify(scheduleService).add(captor.capture());
    ProgramSchedule added = captor.getValue();

    Assert.assertEquals("mySched", added.getName());
    Assert.assertEquals("", added.getDescription());
    Assert.assertEquals(Collections.emptyMap(), added.getProperties());
    Assert.assertEquals(Collections.emptyList(), added.getConstraints());
    Assert.assertEquals(Schedulers.JOB_QUEUE_TIMEOUT_MILLIS, added.getTimeoutMillis());
  }

  @Test
  public void testAddSchedulePreservesExplicitOptionalFields() throws Exception {
    ProgramScheduleService scheduleService = Mockito.mock(ProgramScheduleService.class);
    ProgramLifecycleService lifecycleService = Mockito.mock(ProgramLifecycleService.class);
    Store store = Mockito.mock(Store.class);

    ProgramScheduleHttpHandler handler =
        new ProgramScheduleHttpHandler(scheduleService, lifecycleService, store);

    ConcurrencyConstraint constraint = new ConcurrencyConstraint(3);
    ScheduleDetail explicitDetail = new ScheduleDetail(
        "default",
        "myApp",
        "explicitSched",
        "hourly run",
        new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "myWorkflow"),
        ImmutableMap.of("env", "prod"),
        new ProtoTrigger.TimeTrigger("0 * * * *"),
        Collections.singletonList(constraint),
        90000L,
        null,
        null);

    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.PUT,
        "/v3/namespaces/default/apps/myApp/schedules/explicitSched",
        Unpooled.copiedBuffer(
            ProgramHandlerUtil.toJson(explicitDetail), StandardCharsets.UTF_8));
    MockResponder responder = new MockResponder();

    handler.addSchedule(request, responder, "default", "myApp", "explicitSched");
    Assert.assertEquals(HttpResponseStatus.OK, responder.getStatus());

    ArgumentCaptor<ProgramSchedule> captor = ArgumentCaptor.forClass(ProgramSchedule.class);
    Mockito.verify(scheduleService).add(captor.capture());
    ProgramSchedule added = captor.getValue();

    Assert.assertEquals("hourly run", added.getDescription());
    Assert.assertEquals(ImmutableMap.of("env", "prod"), added.getProperties());
    Assert.assertEquals(1, added.getConstraints().size());
    Assert.assertEquals(90000L, added.getTimeoutMillis());
  }
}
