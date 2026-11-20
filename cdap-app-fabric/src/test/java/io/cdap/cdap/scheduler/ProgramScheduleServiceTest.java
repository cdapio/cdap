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

package io.cdap.cdap.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.ConcurrencyConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ProgramScheduleService}.
 */
public class ProgramScheduleServiceTest {

  @Test
  public void testUpdateFallsBackToExistingFieldsWhenDetailFieldsAreNull() throws Exception {
    AccessEnforcer accessEnforcer = Mockito.mock(AccessEnforcer.class);
    AuthenticationContext authContext = Mockito.mock(AuthenticationContext.class);
    Principal principal = new Principal("alice", Principal.PrincipalType.USER);
    Mockito.when(authContext.getPrincipal()).thenReturn(principal);
    Scheduler scheduler = Mockito.mock(Scheduler.class);
    TimeSchedulerService timeScheduler = Mockito.mock(TimeSchedulerService.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("app1");
    ProgramId workflowId = appId.workflow("wf1");
    ScheduleId scheduleId = appId.schedule("sched1");
    Map<String, String> existingProps = ImmutableMap.of("k1", "v1");
    TimeTrigger existingTrigger = new TimeTrigger("0 * * * *");
    List<ProtoConstraint> existingConstraints = ImmutableList.of(new ConcurrencyConstraint(2));
    long existingTimeout = 60000L;

    ProgramSchedule existing = new ProgramSchedule(
        "sched1", "existing-desc", workflowId, existingProps,
        existingTrigger, existingConstraints, existingTimeout);
    Mockito.when(scheduler.getSchedule(scheduleId)).thenReturn(existing);

    ProgramScheduleService service =
        new ProgramScheduleService(accessEnforcer, authContext, scheduler, timeScheduler);

    // Partial update with null optional fields falls back to existing schedule values.
    ScheduleDetail emptyDetail =
        new ScheduleDetail("sched1", null, null, null, null, null, null);
    service.update(scheduleId, emptyDetail);

    Mockito.verify(accessEnforcer).enforce(appId, principal, ApplicationPermission.EXECUTE);
    ArgumentCaptor<ProgramSchedule> captor = ArgumentCaptor.forClass(ProgramSchedule.class);
    Mockito.verify(scheduler).updateSchedule(captor.capture());
    ProgramSchedule merged = captor.getValue();
    Assert.assertEquals("sched1", merged.getName());
    Assert.assertEquals("existing-desc", merged.getDescription());
    Assert.assertEquals(workflowId, merged.getProgramId());
    Assert.assertEquals(existingProps, merged.getProperties());
    Assert.assertEquals(existingTrigger, merged.getTrigger());
    Assert.assertEquals(existingConstraints, merged.getConstraints());
    Assert.assertEquals(existingTimeout, merged.getTimeoutMillis());

    // Partial ScheduleProgramInfo with null programType and null programName also falls back.
    Mockito.reset(scheduler);
    Mockito.when(scheduler.getSchedule(scheduleId)).thenReturn(existing);
    ScheduleDetail nullProgramFieldsDetail = new ScheduleDetail(
        "sched1", "new-desc", new ScheduleProgramInfo(null, null),
        ImmutableMap.of("k2", "v2"), new TimeTrigger("5 * * * *"),
        ImmutableList.of(new ConcurrencyConstraint(5)), 120000L);
    service.update(scheduleId, nullProgramFieldsDetail);

    ArgumentCaptor<ProgramSchedule> overrideCaptor = ArgumentCaptor.forClass(ProgramSchedule.class);
    Mockito.verify(scheduler).updateSchedule(overrideCaptor.capture());
    ProgramSchedule overridden = overrideCaptor.getValue();
    Assert.assertEquals("new-desc", overridden.getDescription());
    Assert.assertEquals(workflowId, overridden.getProgramId());
    Assert.assertEquals(ImmutableMap.of("k2", "v2"), overridden.getProperties());
    Assert.assertEquals(new TimeTrigger("5 * * * *"), overridden.getTrigger());
    Assert.assertEquals(120000L, overridden.getTimeoutMillis());
  }

  @Test
  public void testUpdateRejectsChangingTargetProgram() throws Exception {
    AccessEnforcer accessEnforcer = Mockito.mock(AccessEnforcer.class);
    AuthenticationContext authContext = Mockito.mock(AuthenticationContext.class);
    Mockito.when(authContext.getPrincipal())
        .thenReturn(new Principal("alice", Principal.PrincipalType.USER));
    Scheduler scheduler = Mockito.mock(Scheduler.class);
    TimeSchedulerService timeScheduler = Mockito.mock(TimeSchedulerService.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("app1");
    ProgramId workflowId = appId.workflow("wf1");
    ScheduleId scheduleId = appId.schedule("sched1");
    ProgramSchedule existing = new ProgramSchedule(
        "sched1", "desc", workflowId, ImmutableMap.of(),
        new TimeTrigger("0 * * * *"), ImmutableList.of());
    Mockito.when(scheduler.getSchedule(scheduleId)).thenReturn(existing);

    ProgramScheduleService service =
        new ProgramScheduleService(accessEnforcer, authContext, scheduler, timeScheduler);

    ScheduleDetail differentProgramDetail = new ScheduleDetail(
        "sched1", "desc", new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW, "otherWf"),
        null, null, null, null);
    try {
      service.update(scheduleId, differentProgramDetail);
      Assert.fail("Expected BadRequestException when changing schedule target program");
    } catch (BadRequestException expected) {
      Mockito.verify(scheduler, Mockito.never()).updateSchedule(Mockito.any());
    }
  }
}
