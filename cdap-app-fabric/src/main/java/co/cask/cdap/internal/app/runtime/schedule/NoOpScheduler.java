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
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.ProgramId;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Noop scheduler.
 */
public class NoOpScheduler implements Scheduler {
  @Override
  public void addProgramSchedule(ProgramSchedule schedule) throws AlreadyExistsException {

  }

  @Override
  public void deleteProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {

  }

  @Override
  public void suspendProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {

  }

  @Override
  public void resumeProgramSchedule(ProgramSchedule schedule) throws NotFoundException, SchedulerException {

  }

  @Override
  public void schedule(ProgramId program, SchedulableProgramType programType, Schedule schedule,
                       Map<String, String> properties) throws SchedulerException {

  }

  @Override
  public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return Collections.emptyList();
  }

  @Override
  public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program, SchedulableProgramType programType)
    throws SchedulerException {
    return Collections.emptyList();
  }

  @Override
  public void suspendSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void resumeSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedule(ProgramId programId, SchedulableProgramType programType, String scheduleName)
    throws NotFoundException, SchedulerException {

  }

  @Override
  public void deleteSchedules(ProgramId programId, SchedulableProgramType programType) throws SchedulerException {

  }

  @Override
  public ProgramScheduleStatus scheduleState(ProgramId program, SchedulableProgramType programType,
                                             String scheduleName) throws ScheduleNotFoundException {
    throw new ScheduleNotFoundException(program.getParent().schedule(scheduleName));
  }
}
