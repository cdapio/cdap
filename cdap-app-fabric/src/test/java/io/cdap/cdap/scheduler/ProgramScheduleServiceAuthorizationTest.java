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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.OrTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Authorization tests for program status triggers in {@link ProgramScheduleService}.
 */
public class ProgramScheduleServiceAuthorizationTest {

  private static final ApplicationId APP_ID = new ApplicationId("ns", "app");
  private static final ProgramId WORKFLOW_ID = APP_ID.workflow("wf");
  private static final ProgramId VISIBLE_UPSTREAM = new ApplicationId("ns", "visible")
      .workflow("wf");
  private static final ProgramId HIDDEN_UPSTREAM = new ApplicationId("other", "hidden")
      .workflow("wf");
  private static final Principal USER = new Principal("wp1-scheduler", Principal.PrincipalType.USER);

  private static InMemoryAccessController accessController;

  private Scheduler scheduler;
  private ProgramScheduleService service;

  @BeforeClass
  public static void setupClass() {
    accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(APP_ID), USER,
        ImmutableSet.of(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(WORKFLOW_ID), USER,
        ImmutableSet.of(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(VISIBLE_UPSTREAM), USER,
        ImmutableSet.of(StandardPermission.GET));
  }

  @Before
  public void setup() {
    scheduler = mock(Scheduler.class);
    service = new ProgramScheduleService(accessController, new AuthenticationTestContext(),
        scheduler, mock(TimeSchedulerService.class));
    AuthenticationTestContext.actAsPrincipal(USER);
  }

  @Test
  public void testAddScheduleChecksTriggeringPrograms() throws Exception {
    try {
      service.add(schedule("hidden", HIDDEN_UPSTREAM));
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    verify(scheduler, never()).addSchedule(any());

    ProgramSchedule visible = schedule("visible", VISIBLE_UPSTREAM);
    service.add(visible);
    verify(scheduler).addSchedule(visible);
  }

  @Test
  public void testUpdateScheduleChecksTriggeringPrograms() throws Exception {
    ScheduleId scheduleId = APP_ID.schedule("sched");
    try {
      service.update(scheduleId, detail(HIDDEN_UPSTREAM));
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    verify(scheduler, never()).getSchedule(any());
    verify(scheduler, never()).updateSchedule(any());

    when(scheduler.getSchedule(scheduleId)).thenReturn(schedule("sched", VISIBLE_UPSTREAM));
    service.update(scheduleId, detail(VISIBLE_UPSTREAM));
    verify(scheduler).updateSchedule(any());
  }

  @Test
  public void testFindTriggeredByFiltersInvisibleSchedules() throws Exception {
    ProgramScheduleMeta meta = new ProgramScheduleMeta(ProgramScheduleStatus.SCHEDULED, 0L);
    ProgramScheduleRecord visible = new ProgramScheduleRecord(
        schedule("visible", VISIBLE_UPSTREAM), meta);
    ProgramId hiddenProgram = new ApplicationId("other", "app").workflow("wf");
    ProgramScheduleRecord hidden = new ProgramScheduleRecord(
        new ProgramSchedule("hidden", "", hiddenProgram, Collections.emptyMap(),
            new ProgramStatusTrigger(VISIBLE_UPSTREAM, ProgramStatus.COMPLETED),
            Collections.emptyList()), meta);
    when(scheduler.findSchedules(anyString())).thenReturn(Arrays.asList(visible, hidden));

    Collection<ProgramScheduleRecord> result = service.findTriggeredBy(VISIBLE_UPSTREAM,
        Collections.singleton(ProgramStatus.COMPLETED));
    Assert.assertEquals(Collections.singleton(visible), result);
  }

  @Test
  public void testEnforceTriggeringProgramsWithFilter() {
    // Used at app deployment time, where triggers on programs of the deployed app are skipped
    ProgramId sameAppHidden = APP_ID.workflow("otherwf");
    ProgramScheduleService.enforceTriggeringPrograms(accessController, USER,
        schedule("same", sameAppHidden).getTrigger(),
        program -> !program.getAppReference().equals(APP_ID.getAppReference()));
    try {
      ProgramScheduleService.enforceTriggeringPrograms(accessController, USER,
          schedule("hidden", HIDDEN_UPSTREAM).getTrigger(),
          program -> !program.getAppReference().equals(APP_ID.getAppReference()));
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  /**
   * Creates a schedule for the workflow that is triggered by a time trigger or by the completion
   * of the given upstream program.
   */
  private static ProgramSchedule schedule(String name, ProgramId upstream) {
    return new ProgramSchedule(name, "", WORKFLOW_ID, Collections.emptyMap(),
        new OrTrigger(new TimeTrigger("0 * * * *"),
            new ProgramStatusTrigger(upstream, ProgramStatus.COMPLETED)),
        Collections.emptyList());
  }

  private static ScheduleDetail detail(ProgramId upstream) {
    return new ScheduleDetail(null, "new description", null, null,
        new OrTrigger(new TimeTrigger("0 * * * *"),
            new ProgramStatusTrigger(upstream, ProgramStatus.COMPLETED)), null, null);
  }
}
