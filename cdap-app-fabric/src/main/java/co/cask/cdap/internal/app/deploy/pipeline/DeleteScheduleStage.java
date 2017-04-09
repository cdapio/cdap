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

import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ScheduleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for deletion of any schedule which was dropped
 * during application re-deployment. Schedules are created and updated in {@link CreateSchedulesStage}.
 * These two stages are separate to avoid schedule triggers before app registration. See CDAP-8918 for details
 */
public class DeleteScheduleStage extends AbstractStage<ApplicationWithPrograms> {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteScheduleStage.class);
  private final Scheduler scheduler;

  public DeleteScheduleStage(Scheduler scheduler) {
    super(TypeToken.of(ApplicationWithPrograms.class));
    this.scheduler = scheduler;
  }

  @Override
  public void process(ApplicationWithPrograms input) throws Exception {
    // drop deleted schedules
    Map<String, ScheduleSpecification> existingSchedulesMap = input.getExistingAppSpec() != null ?
      input.getExistingAppSpec().getSchedules() : ImmutableMap.<String, ScheduleSpecification>of();
    MapDifference<String, ScheduleSpecification> mapDiff = Maps.difference(existingSchedulesMap,
                                                                           input.getSpecification().getSchedules());
    for (Map.Entry<String, ScheduleSpecification> entry : mapDiff.entriesOnlyOnLeft().entrySet()) {
      // delete schedules that existed in the old app spec, but don't anymore
      ScheduleSpecification scheduleSpec = entry.getValue();
      ProgramType programType = ProgramType.valueOfSchedulableType(scheduleSpec.getProgram().getProgramType());
      LOG.debug("Deleting schedule {} with specification {}", entry.getKey(), scheduleSpec);
      scheduler.deleteSchedule(input.getApplicationId().program(programType,
                                                                scheduleSpec.getProgram().getProgramName()),
                               scheduleSpec.getProgram().getProgramType(), scheduleSpec.getSchedule().getName());
    }

    // for the entries which differ schedule type delete them here so that we can re-create them again in
    // CreateScheduleStage with new type and spec. Its required to delete it here because in next stage the
    // application will be registered updating the schedule spec. See CDAP-8918 for details.
    for (Map.Entry<String, MapDifference.ValueDifference<ScheduleSpecification>> entry :
      mapDiff.entriesDiffering().entrySet()) {
      ScheduleSpecification newScheduleSpec = entry.getValue().rightValue();
      ScheduleSpecification oldScheduleSpec = entry.getValue().leftValue();
      if (ScheduleType.fromSchedule(newScheduleSpec.getSchedule()) !=
        ScheduleType.fromSchedule(oldScheduleSpec.getSchedule())) {
        LOG.debug("Deleting existing schedule {} with specification {}. It will be created with specification {} " +
                    "as they differ in schedule type.", entry.getKey(), oldScheduleSpec, newScheduleSpec);
        ProgramType programType = ProgramType.valueOfSchedulableType(newScheduleSpec.getProgram().getProgramType());
        scheduler.deleteSchedule(input.getApplicationId().program(programType,
                                                                  oldScheduleSpec.getProgram().getProgramName()),
                                 oldScheduleSpec.getProgram().getProgramType(),
                                 oldScheduleSpec.getSchedule().getName());
      }
    }
    // Emit the input to next stage.
    emit(input);
  }
}
