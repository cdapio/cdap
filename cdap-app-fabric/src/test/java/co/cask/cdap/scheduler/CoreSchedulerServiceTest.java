/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.scheduler;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

public class CoreSchedulerServiceTest extends AppFabricTestBase {

  private static final NamespaceId NS_ID = new NamespaceId("schedtest");
  private static final ApplicationId APP1_ID = NS_ID.app("app1");
  private static final ApplicationId APP2_ID = NS_ID.app("app2");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final WorkflowId PROG11_ID = APP1_ID.workflow("wf11");
  private static final ScheduleId PSCHED1_ID = APP1_ID.schedule("psched1");
  private static final ScheduleId PSCHED2_ID = APP2_ID.schedule("psched2");
  private static final ScheduleId TSCHED1_ID = APP1_ID.schedule("tsched1");
  private static final ScheduleId TSCHED11_ID = APP1_ID.schedule("tsched11");
  private static final DatasetId DS1_ID = NS_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS_ID.dataset("pfs2");

  @Test
  public void addListDeleteSchedules() throws Exception {
    Scheduler scheduler = getInjector().getInstance(Scheduler.class);

    // verify that list returns nothing
    Assert.assertTrue(scheduler.listSchedules(APP1_ID).isEmpty());
    Assert.assertTrue(scheduler.listSchedules(PROG1_ID).isEmpty());

    // add a schedule for app1
    ProgramSchedule tsched1 = new ProgramSchedule("tsched1", "one time schedule", PROG1_ID,
                                                  ImmutableMap.of("prop1", "nn"),
                                                  new TimeTrigger("* * * * 1"), ImmutableList.<Constraint>of());
    scheduler.addSchedule(tsched1);
    Assert.assertEquals(tsched1, scheduler.getSchedule(TSCHED1_ID));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(PROG1_ID));

    // add three more schedules, one for the same program, one for the same app, one for another app
    ProgramSchedule psched1 = new ProgramSchedule("psched1", "one partition schedule", PROG1_ID,
                                                  ImmutableMap.of("prop3", "abc"),
                                                  new PartitionTrigger(DS1_ID, 1), ImmutableList.<Constraint>of());
    ProgramSchedule tsched11 = new ProgramSchedule("tsched11", "two times schedule", PROG11_ID,
                                                   ImmutableMap.of("prop2", "xx"),
                                                   new TimeTrigger("* * * * 1,2"), ImmutableList.<Constraint>of());
    ProgramSchedule psched2 = new ProgramSchedule("psched2", "two partition schedule", PROG2_ID,
                                                  ImmutableMap.of("propper", "popper"),
                                                  new PartitionTrigger(DS2_ID, 2), ImmutableList.<Constraint>of());
    scheduler.addSchedules(ImmutableList.of(psched1, tsched11, psched2));
    Assert.assertEquals(psched1, scheduler.getSchedule(PSCHED1_ID));
    Assert.assertEquals(tsched11, scheduler.getSchedule(TSCHED11_ID));
    Assert.assertEquals(psched2, scheduler.getSchedule(PSCHED2_ID));

    // list by app and program
    Assert.assertEquals(ImmutableList.of(psched1, tsched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // delete one schedule
    scheduler.deleteSchedule(TSCHED1_ID);
    verifyNotFound(scheduler, TSCHED1_ID);
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // attempt to delete it again along with another one that exists
    try {
      scheduler.deleteSchedules(ImmutableList.of(TSCHED1_ID, TSCHED11_ID));
      Assert.fail("expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));


    // attempt to add it back together with a schedule that exists
    try {
      scheduler.addSchedules(ImmutableList.of(tsched1, tsched11));
      Assert.fail("expected AlreadyExistsException");
    } catch (AlreadyExistsException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // add it back, delete all schedules for one app
    scheduler.addSchedule(tsched1);
    scheduler.deleteSchedules(APP1_ID);
    verifyNotFound(scheduler, TSCHED1_ID);
    verifyNotFound(scheduler, PSCHED1_ID);
    verifyNotFound(scheduler, TSCHED11_ID);
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
  }

  private static void verifyNotFound(Scheduler scheduler, ScheduleId scheduleId) {
    try {
      scheduler.getSchedule(scheduleId);
      Assert.fail("expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }
  }
}
