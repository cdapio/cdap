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
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Responsible for creating program schedules from an ApplicationSpecification.
 */
public class CreateProgramSchedulesStage extends AbstractStage<ApplicationWithPrograms> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateProgramSchedulesStage.class);

  private final Scheduler programScheduler;

  public CreateProgramSchedulesStage(Scheduler programScheduler) {
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

    Map<String, ScheduleCreationSpec> programSchedules = input.getSpecification().getProgramSchedules();
    for (final Map.Entry<String, ScheduleCreationSpec> entry : programSchedules.entrySet()) {
      ScheduleCreationSpec scheduleCreationSpec = entry.getValue();
      ProgramId programId = input.getApplicationId().workflow(scheduleCreationSpec.getProgramName());
      ProtoTrigger trigger = (ProtoTrigger) scheduleCreationSpec.getTrigger();
      ProgramSchedule programSchedule =
        new ProgramSchedule(scheduleCreationSpec.getName(), scheduleCreationSpec.getDescription(), programId,
                            scheduleCreationSpec.getProperties(), trigger, scheduleCreationSpec.getConstraints());
      programScheduler.addSchedule(programSchedule);
    }

    // Emit the input to next stage.
    emit(input);
  }
}
