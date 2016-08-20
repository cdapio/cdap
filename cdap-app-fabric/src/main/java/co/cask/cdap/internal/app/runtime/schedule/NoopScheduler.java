/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ScheduledRuntime;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Noop scheduler.
 */
public class NoopScheduler implements Scheduler {
  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType,
                       Schedule schedule) throws SchedulerException {

  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {

  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType,
                       Iterable<Schedule> schedules) throws SchedulerException {

  }

  @Override
  public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules,
                       Map<String, String> properties) throws SchedulerException {

  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(Id.Program program,
                                                         SchedulableProgramType programType) throws SchedulerException {
    return new ArrayList<>();
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program,
                                                     SchedulableProgramType programType) throws SchedulerException {
    return new ArrayList<>();
  }

  @Override
  public List<String> getScheduleIds(Id.Program program,
                                     SchedulableProgramType programType) throws SchedulerException {
    return new ArrayList<>();
  }

  @Override
  public void suspendSchedule(Id.Program program, SchedulableProgramType programType,
                              String scheduleName) throws NotFoundException, SchedulerException {

  }

  @Override
  public void resumeSchedule(Id.Program program, SchedulableProgramType programType,
                             String scheduleName) throws NotFoundException, SchedulerException {

  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType,
                             Schedule schedule) throws NotFoundException, SchedulerException {

  }

  @Override
  public void updateSchedule(Id.Program program, SchedulableProgramType programType, Schedule schedule,
                             Map<String, String> properties) throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedule(Id.Program programId, SchedulableProgramType programType,
                             String scheduleName) throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedules(Id.Program programId, SchedulableProgramType programType) throws SchedulerException {

  }

  @Override
  public void deleteAllSchedules(Id.Namespace namespaceId) throws SchedulerException {

  }

  @Override
  public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType,
                                     String scheduleName) throws SchedulerException {
    return null;
  }
}
