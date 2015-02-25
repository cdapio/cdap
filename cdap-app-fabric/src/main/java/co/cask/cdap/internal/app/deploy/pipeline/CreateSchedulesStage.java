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
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import com.google.common.reflect.TypeToken;

import java.util.Map;

/**
 * This {@link co.cask.cdap.pipeline.Stage} is responsible for automatic creation of any new schedules
 * specified by the application. If the schedules already exist, it will update them.
 */
public class CreateSchedulesStage extends AbstractStage<ApplicationDeployable> {

  private final Store store;
  private final Scheduler scheduler;

  public CreateSchedulesStage(Store store, Scheduler scheduler) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.scheduler = scheduler;
  }

  @Override
  public void process(ApplicationDeployable appSpec) throws Exception {
    ApplicationSpecification existingAppSpec = store.getApplication(appSpec.getId());
    for (Map.Entry<String, ScheduleSpecification> entry : appSpec.getSpecification().getSchedules().entrySet()) {
      ScheduleSpecification scheduleSpec = entry.getValue();
      if (existingAppSpec != null && existingAppSpec.getSchedules().containsKey(entry.getKey())) {
        scheduler.updateSchedule(Id.Program.from(appSpec.getId(), scheduleSpec.getProgram().getProgramName()),
                                 scheduleSpec.getProgram().getProgramType(),
                                 scheduleSpec.getSchedule());
      } else {
        scheduler.schedule(Id.Program.from(appSpec.getId(), scheduleSpec.getProgram().getProgramName()),
                           scheduleSpec.getProgram().getProgramType(),
                           scheduleSpec.getSchedule());
      }
    }

    // Emit the input to next stage.
    emit(appSpec);
  }
}
