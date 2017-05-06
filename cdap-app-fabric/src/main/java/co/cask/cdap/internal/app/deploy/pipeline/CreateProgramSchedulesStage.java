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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 * Responsible for creating program schedules from an ApplicationSpecification.
 */
public class CreateProgramSchedulesStage extends AbstractStage<ApplicationWithPrograms> {

  private final Scheduler programScheduler;
  private final co.cask.cdap.internal.app.runtime.schedule.Scheduler scheduler;

  public CreateProgramSchedulesStage(Scheduler programScheduler,
                                     co.cask.cdap.internal.app.runtime.schedule.Scheduler scheduler) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.programScheduler = programScheduler;
    this.scheduler = scheduler;
  }

  @Override
  public void process(final ApplicationWithPrograms input) throws Exception {

    if (!input.canUpdateSchedules()) {
      // if we cant update schedules, emit and return
      emit(input);
      return;
    }

    Map<String, ScheduleCreationSpec> programSchedules = input.getSpecification().getProgramSchedules();
    for (final Map.Entry<String, ScheduleCreationSpec> entry : programSchedules.entrySet()) {
      ScheduleCreationSpec scheduleCreationSpec = entry.getValue();
      ProgramId programId = input.getApplicationId().workflow(scheduleCreationSpec.getProgramName());
      Trigger trigger = scheduleCreationSpec.getTrigger();
      ProgramSchedule programSchedule =
        new ProgramSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(),
                            programId, scheduleCreationSpec.getProperties(), trigger,
                            scheduleCreationSpec.getConstraints());
      programScheduler.addSchedule(programSchedule);
      if (trigger instanceof TimeTrigger) {
        Schedule timeSchedule = new TimeSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(),
                                                 ((TimeTrigger) trigger).getCronExpression());
        scheduler.schedule(programId, SchedulableProgramType.WORKFLOW, timeSchedule);
      }
    }

    // Emit the input to next stage.
    emit(input);
  }
}
