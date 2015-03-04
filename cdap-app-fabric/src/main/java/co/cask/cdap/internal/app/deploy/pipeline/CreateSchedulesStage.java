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

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new schedules
 * specified by the application. If the schedules already exist, it will update them.
 */
public class CreateSchedulesStage extends AbstractStage<ApplicationWithPrograms> {

  private final Scheduler scheduler;

  public CreateSchedulesStage(Scheduler scheduler) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.scheduler = scheduler;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    ApplicationSpecification existingAppSpec = input.getExistingAppSpecification();
    Map<String, ScheduleSpecification> existingSchedulesMap;
    if (existingAppSpec == null) {
      existingSchedulesMap = ImmutableMap.of();
    } else {
      existingSchedulesMap = existingAppSpec.getSchedules();
    }

    MapDifference<String, ScheduleSpecification> mapDiff =
      Maps.difference(existingSchedulesMap, input.getSpecification().getSchedules());
    for (Map.Entry<String, ScheduleSpecification> entry : mapDiff.entriesOnlyOnLeft().entrySet()) {
      // delete schedules that existed in the old app spec, but don't anymore
      ScheduleSpecification scheduleSpec = entry.getValue();
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      scheduler.deleteSchedule(Id.Program.from(input.getId(), programType, scheduleSpec.getProgram().getProgramName()),
                               scheduleSpec.getProgram().getProgramType(),
                               scheduleSpec.getSchedule().getName());
    }

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

      ProgramType programType = ProgramType.valueOfSchedulableType(newScheduleSpec.getProgram().getProgramType());
      scheduler.updateSchedule(Id.Program.from(input.getId(), programType,
                                               newScheduleSpec.getProgram().getProgramName()),
                               newScheduleSpec.getProgram().getProgramType(),
                               newScheduleSpec.getSchedule());
    }

    for (Map.Entry<String, ScheduleSpecification> entry : mapDiff.entriesOnlyOnRight().entrySet()) {
      ScheduleSpecification scheduleSpec = entry.getValue();
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      scheduler.schedule(Id.Program.from(input.getId(), programType, scheduleSpec.getProgram().getProgramName()),
                         scheduleSpec.getProgram().getProgramType(),
                         scheduleSpec.getSchedule());
    }

    // Note: the mapDiff also has a entriesInCommon method returning all entries in left and right maps
    // which have exactly the same keys and values. In that case, we don't need to do anything, not
    // even to update the schedule

    // Emit the input to next stage.
    emit(input);
  }
}
