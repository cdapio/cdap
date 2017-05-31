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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.Set;

/**
 * Responsible for deleting dropped or updated schedules and creating new schedules
 */
public class DeleteAndCreateSchedulesStage extends AbstractStage<ApplicationWithPrograms> {

  private final Scheduler programScheduler;

  public DeleteAndCreateSchedulesStage(Scheduler programScheduler) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.programScheduler = programScheduler;
  }

  @Override
  public void process(final ApplicationWithPrograms input) throws Exception {

    if (!input.canUpdateSchedules()) {
      // if we cant update schedules, emit and return
      emit(input);
      return;
    }

    ApplicationId appId = input.getApplicationId();
    // Get a set of new schedules from the app spec
    Set<ProgramSchedule> newSchedules = getProgramScheduleSet(appId, input.getSpecification());

    for (ProgramSchedule schedule : programScheduler.listSchedules(appId)) {
      if (newSchedules.contains(schedule)) {
        newSchedules.remove(schedule); // Remove the existing schedule from the newSchedules
        continue;
      }
      // Delete the existing schedule if it is not present in newSchedules
      programScheduler.deleteSchedule(schedule.getScheduleId());
    }

    // Add new schedules
    for (ProgramSchedule schedule : newSchedules) {
      addSchedule(schedule);
    }

    // Emit the input to next stage.
    emit(input);
  }

  private Set<ProgramSchedule> getProgramScheduleSet(ApplicationId appId, ApplicationSpecification appSpec) {
    Set<ProgramSchedule> schedules = new HashSet<>();
    for (ScheduleCreationSpec scheduleCreationSpec : appSpec.getProgramSchedules().values()) {
      schedules.add(toProgramSchedule(appId, scheduleCreationSpec));
    }
    return schedules;
  }

  private ProgramSchedule toProgramSchedule(ApplicationId appId, ScheduleCreationSpec scheduleCreationSpec) {
    ProgramId programId = appId.workflow(scheduleCreationSpec.getProgramName());
    Trigger trigger = scheduleCreationSpec.getTrigger();
    return new ProgramSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(), programId,
                               scheduleCreationSpec.getProperties(), trigger, scheduleCreationSpec.getConstraints(),
                               scheduleCreationSpec.getTimeoutMillis());
  }

  private void addSchedule(ProgramSchedule programSchedule) throws AlreadyExistsException, BadRequestException {
    programScheduler.addSchedule(programSchedule);
  }
}
