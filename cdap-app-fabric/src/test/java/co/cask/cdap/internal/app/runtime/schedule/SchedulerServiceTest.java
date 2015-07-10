/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SchedulerServiceTest {
  private static SchedulerService schedulerService;
  private static Store store;
  private static NamespaceAdmin namespaceAdmin;
  private static final Id.Namespace namespace = new Id.Namespace("notdefault");
  private static final Id.Application appId = new Id.Application(namespace, AppWithWorkflow.NAME);
  private static final Id.Program program = new Id.Program(appId, ProgramType.WORKFLOW,
                                                           AppWithWorkflow.SampleWorkflow.NAME);
  private static final SchedulableProgramType programType = SchedulableProgramType.WORKFLOW;
  private static final Id.Stream STREAM_ID = Id.Stream.from(namespace, "stream");
  private static final Schedule timeSchedule1 = Schedules.createTimeSchedule("Schedule1", "Every minute", "* * * * ?");
  private static final Schedule timeSchedule2 = Schedules.createTimeSchedule("Schedule2", "Every Hour", "0 * * * ?");
  private static final Schedule dataSchedule1 =
    Schedules.createDataSchedule("Schedule3", "Every 1M", Schedules.Source.STREAM, STREAM_ID.getId(), 1);
  private static final Schedule dataSchedule2 =
    Schedules.createDataSchedule("Schedule4", "Every 10M", Schedules.Source.STREAM, STREAM_ID.getId(), 10);

  @BeforeClass
  public static void set() throws Exception {
    schedulerService = AppFabricTestHelper.getInjector().getInstance(SchedulerService.class);
    store = AppFabricTestHelper.getInjector().getInstance(Store.class);
    namespaceAdmin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    namespaceAdmin.createNamespace(new NamespaceMeta.Builder().setName(namespace).build());
    namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
  }

  @AfterClass
  public static void finish() throws NotFoundException, NamespaceCannotBeDeletedException {
    namespaceAdmin.deleteNamespace(namespace);
    namespaceAdmin.deleteDatasets(Constants.DEFAULT_NAMESPACE_ID);
    schedulerService.stopAndWait();
  }

  @Test
  public void testSchedulesAcrossNamespace() throws Exception {
    AppFabricTestHelper.deployApplication(namespace, AppWithWorkflow.class);

    schedulerService.schedule(program, programType, ImmutableList.of(timeSchedule1));
    Assert.assertEquals(1, getSchedules(appId).size());
    List<String> scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(1, scheduleIds.size());

    Id.Namespace otherNamespace = Id.Namespace.from("otherNamespace");
    Id.Application appInOtherNamespace = Id.Application.from(otherNamespace, appId.getId());
    Id.Program programInOtherNamespace = Id.Program.from(appInOtherNamespace, program.getType(), program.getId());

    List<String> scheduleIdsOtherNamespace = schedulerService.getScheduleIds(programInOtherNamespace, programType);
    Assert.assertEquals(0, scheduleIdsOtherNamespace.size());

    namespaceAdmin.createNamespace(new NamespaceMeta.Builder().setName(otherNamespace).build());
    AppFabricTestHelper.deployApplication(otherNamespace, AppWithWorkflow.class);
    schedulerService.schedule(programInOtherNamespace, programType, ImmutableList.of(timeSchedule2));
    Assert.assertEquals(1, getSchedules(appInOtherNamespace).size());
    scheduleIdsOtherNamespace = schedulerService.getScheduleIds(programInOtherNamespace, programType);
    Assert.assertEquals(1, scheduleIdsOtherNamespace.size());

    Assert.assertNotEquals(scheduleIds.get(0), scheduleIdsOtherNamespace.get(0));

    schedulerService.deleteSchedule(program, programType, timeSchedule1.getName());
    Assert.assertEquals(0, getSchedules(appId).size());
    schedulerService.deleteAllSchedules(otherNamespace);
    Assert.assertEquals(0, getSchedules(appInOtherNamespace).size());
    namespaceAdmin.deleteNamespace(otherNamespace);
  }

  @Test
  public void testSimpleSchedulerLifecycle() throws Exception {
    AppFabricTestHelper.deployApplication(namespace, AppWithWorkflow.class);

    schedulerService.schedule(program, programType, ImmutableList.of(timeSchedule1));
    List<String> scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(1, scheduleIds.size());
    Assert.assertEquals(1, getSchedules(appId).size());

    checkState(Scheduler.ScheduleState.SUSPENDED, scheduleIds);
    schedulerService.resumeSchedule(program, programType, "Schedule1");
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.schedule(program, programType, ImmutableList.of(timeSchedule2));
    scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(2, scheduleIds.size());
    Assert.assertEquals(2, getSchedules(appId).size());
    schedulerService.resumeSchedule(program, programType, "Schedule2");
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.schedule(program, programType, ImmutableList.of(dataSchedule1, dataSchedule2));
    scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(4, scheduleIds.size());
    Assert.assertEquals(4, getSchedules(appId).size());
    schedulerService.resumeSchedule(program, programType, "Schedule3");
    schedulerService.resumeSchedule(program, programType, "Schedule4");
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule1");
    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule2");

    checkState(Scheduler.ScheduleState.SUSPENDED, ImmutableList.of("Schedule1", "Schedule2"));
    checkState(Scheduler.ScheduleState.SCHEDULED, ImmutableList.of("Schedule3", "Schedule4"));

    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule3");
    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule4");

    checkState(Scheduler.ScheduleState.SUSPENDED, scheduleIds);

    schedulerService.deleteSchedules(program, programType);
    Assert.assertEquals(0, schedulerService.getScheduleIds(program, programType).size());
    Assert.assertEquals(0, getSchedules(appId).size());
    // Check the state of the old scheduleIds
    // (which should be deleted by the call to SchedulerService#delete(Program, ProgramType)
    checkState(Scheduler.ScheduleState.NOT_FOUND, scheduleIds);
  }

  private void checkState(Scheduler.ScheduleState expectedState, List<String> scheduleIds) throws Exception {
    for (String scheduleId : scheduleIds) {
      int i = scheduleId.lastIndexOf(':');
      Assert.assertEquals(expectedState, schedulerService.scheduleState(program, SchedulableProgramType.WORKFLOW,
                                                                        scheduleId.substring(i + 1)));
    }
  }

  private Map<String, ScheduleSpecification> getSchedules(Id.Application appId) {
    ApplicationSpecification application = store.getApplication(appId);
    Assert.assertNotNull(application);
    return application.getSchedules();
  }
}
