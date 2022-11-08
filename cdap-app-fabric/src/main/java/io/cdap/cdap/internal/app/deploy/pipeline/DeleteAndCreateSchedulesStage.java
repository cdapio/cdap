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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.pipeline.AbstractStage;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.scheduler.Scheduler;

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

    ApplicationReference applicationRef = input.getApplicationId().getAppReference();
    // Get a set of new schedules from the app spec
    Set<ProgramSchedule> newSchedules = getProgramScheduleSet(applicationRef, input.getSpecification());
    for (ProgramSchedule schedule : programScheduler.listSchedules(applicationRef)) {
      if (newSchedules.contains(schedule)) {
        newSchedules.remove(schedule); // Remove the existing schedule from the newSchedules
        continue;
      }
      // Delete the existing schedule if it is not present in newSchedules
      programScheduler.deleteSchedule(schedule.getScheduleId());
    }

    // Add new schedules
    programScheduler.addSchedules(newSchedules);

    // Emit the input to next stage.
    emit(input);
  }

  private Set<ProgramSchedule> getProgramScheduleSet(ApplicationReference applicationRef,
                                                     ApplicationSpecification appSpec) {
    Set<ProgramSchedule> schedules = new HashSet<>();
    for (ScheduleCreationSpec scheduleCreationSpec : appSpec.getProgramSchedules().values()) {
      schedules.add(toProgramSchedule(applicationRef, scheduleCreationSpec));
    }
    return schedules;
  }

  private ProgramSchedule toProgramSchedule(ApplicationReference applicationRef,
                                            ScheduleCreationSpec scheduleCreationSpec) {
    ProgramReference programReference = applicationRef.program(ProgramType.WORKFLOW,
                                                               scheduleCreationSpec.getProgramName());
    Trigger trigger = scheduleCreationSpec.getTrigger();
    return new ProgramSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(), programReference,
                               scheduleCreationSpec.getProperties(), trigger, scheduleCreationSpec.getConstraints(),
                               scheduleCreationSpec.getTimeoutMillis());
  }
}
