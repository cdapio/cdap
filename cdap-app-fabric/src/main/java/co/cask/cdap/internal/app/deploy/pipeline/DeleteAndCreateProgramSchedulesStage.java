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
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Responsible for deleting dropped or updated {@link ProgramSchedule} and creating new {@link ProgramSchedule}
 * from an {@link }.
 */
public class DeleteAndCreateProgramSchedulesStage extends AbstractStage<ApplicationWithPrograms> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteAndCreateProgramSchedulesStage.class);

  private final Scheduler programScheduler;
  private final co.cask.cdap.internal.app.runtime.schedule.Scheduler scheduler;

  public DeleteAndCreateProgramSchedulesStage(Scheduler programScheduler,
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

    // Construct a set of new schedules from the app spec
    Set<ProgramSchedule> newSchedules = new HashSet<>();
    for (ScheduleCreationSpec scheduleCreationSpec : input.getSpecification().getProgramSchedules().values()) {
      newSchedules.add(toProgramSchedule(input, scheduleCreationSpec));
    }

    for (ProgramSchedule schedule : programScheduler.listSchedules(input.getApplicationId())) {
      if (newSchedules.contains(schedule)) {
        newSchedules.remove(schedule); // Remove the existing schedule from the newSchedules
        continue;
      }
      // Delete the existing schedule if it is not present in newSchedules
      programScheduler.deleteSchedule(input.getApplicationId().schedule(schedule.getName()));
      scheduler.deleteProgramSchedule(schedule);
    }

    // Add new schedules
    for (ProgramSchedule schedule : newSchedules) {
      addSchedule(schedule);
    }

    // Emit the input to next stage.
    emit(input);
  }

  private ProgramSchedule toProgramSchedule(ApplicationWithPrograms input, ScheduleCreationSpec scheduleCreationSpec) {
    ProgramId programId = input.getApplicationId().workflow(scheduleCreationSpec.getProgramName());
    ProtoTrigger trigger = (ProtoTrigger) scheduleCreationSpec.getTrigger();
    return new ProgramSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(), programId,
                               scheduleCreationSpec.getProperties(), trigger, scheduleCreationSpec.getConstraints());
  }

  private void addSchedule(ProgramSchedule programSchedule)
    throws AlreadyExistsException, BadRequestException, SchedulerException {

    programScheduler.addSchedule(programSchedule);
    Trigger trigger = programSchedule.getTrigger();
    if (trigger instanceof TimeTrigger) {
      Schedule timeSchedule = new TimeSchedule(programSchedule.getName(), programSchedule.getDescription(),
                                               ((TimeTrigger) trigger).getCronExpression());
      scheduler.schedule(programSchedule.getProgramId(), SchedulableProgramType.WORKFLOW, timeSchedule);
    }
  }
}
