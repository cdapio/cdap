/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduleType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new schedules
 * specified by the application. If the schedules already exist, it will update them. For schedule deletion see
 * {@link DeleteScheduleStage}. They are broken into separate stages because we want schedule creation and update to
 * happen after the app is registered in the app store as it might be possible that a schedule gets triggered instantly
 * after being added when the app is still to be registered. See CDAP-8918 for details.
 */
public class CreateSchedulesStage extends AbstractStage<ApplicationWithPrograms> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSchedulesStage.class);
  private final co.cask.cdap.scheduler.Scheduler programScheduler;

  public CreateSchedulesStage(co.cask.cdap.scheduler.Scheduler programScheduler) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.programScheduler = programScheduler;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {

    if (!input.canUpdateSchedules()) {
      // if we cant update schedules, emit and return
      emit(input);
      return;
    }

    ApplicationId appId = input.getApplicationId();
    Map<String, ScheduleSpecification> existingSchedulesMap = input.getExistingAppSpec() != null ?
      input.getExistingAppSpec().getSchedules() : ImmutableMap.<String, ScheduleSpecification>of();
    MapDifference<String, ScheduleSpecification> mapDiff = Maps.difference(existingSchedulesMap,
                                                                           input.getSpecification().getSchedules());
    for (Map.Entry<String, MapDifference.ValueDifference<ScheduleSpecification>> entry :
      mapDiff.entriesDiffering().entrySet()) {
      // Update those schedules - the new schedules have the same IDs but different specs
      ScheduleSpecification newScheduleSpec = entry.getValue().rightValue();
      ScheduleSpecification oldScheduleSpec = entry.getValue().leftValue();
      if (newScheduleSpec.getSchedule().equals(oldScheduleSpec.getSchedule())) {
        // The schedules are exactly the same - the difference in spec might come from the properties map -
        // hence it is useless to update the schedule
        continue;
      }

      // if the schedule differ in schedule type then we have deleted the existing one earlier in DeleteScheduleStage.
      // create it with the new type and spec here. See CDAP-8918 for details.
      if (ScheduleType.fromSchedule(newScheduleSpec.getSchedule()) !=
        ScheduleType.fromSchedule(oldScheduleSpec.getSchedule())) {
        LOG.debug("Redeploying schedule {} with specification {} which existed earlier with specification {}",
                  entry.getKey(), newScheduleSpec, oldScheduleSpec);
        addProgramSchedule(appId, newScheduleSpec);
        continue;
      }

      ProgramType programType = ProgramType.valueOfSchedulableType(newScheduleSpec.getProgram().getProgramType());
      ProgramId programId = appId.program(programType, newScheduleSpec.getProgram().getProgramName());

      programScheduler.updateSchedule(Schedulers.toProgramSchedule(newScheduleSpec.getSchedule(), programId,
                                                                   newScheduleSpec.getProperties()));
    }

    for (Map.Entry<String, ScheduleSpecification> entry : mapDiff.entriesOnlyOnRight().entrySet()) {
      addProgramSchedule(appId, entry.getValue());
    }

    // Emit the input to next stage.
    emit(input);
  }

  private void addProgramSchedule(ApplicationId appId, ScheduleSpecification scheduleSpec)
    throws AlreadyExistsException, BadRequestException {
    ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
    ProgramId programId = appId.program(programType, scheduleSpec.getProgram().getProgramName());
    Schedule schedule = scheduleSpec.getSchedule();
    ProgramSchedule programSchedule =
      Schedulers.toProgramSchedule(schedule, programId, scheduleSpec.getProperties());
    programScheduler.addSchedule(programSchedule);
  }
}
