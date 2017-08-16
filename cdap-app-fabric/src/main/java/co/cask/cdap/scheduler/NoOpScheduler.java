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

package co.cask.cdap.scheduler;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;

import java.util.Collection;
import java.util.List;

/**
 * Noop scheduler.
 */
public class NoOpScheduler implements Scheduler {
  @Override
  public void addSchedule(ProgramSchedule schedule) throws AlreadyExistsException {

  }

  @Override
  public void addSchedules(Iterable<? extends ProgramSchedule> schedules) throws AlreadyExistsException {

  }

  @Override
  public void updateSchedule(ProgramSchedule schedule) throws NotFoundException {

  }

  @Override
  public void enableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException {

  }

  @Override
  public void disableSchedule(ScheduleId scheduleId) throws NotFoundException, ConflictException {

  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId) throws NotFoundException {

  }

  @Override
  public void deleteSchedules(Iterable<? extends ScheduleId> scheduleIds) throws NotFoundException {

  }

  @Override
  public void deleteSchedules(ApplicationId appId) {

  }

  @Override
  public void deleteSchedules(ProgramId programId) {

  }

  @Override
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException {
    return null;
  }

  @Override
  public ProgramScheduleStatus getScheduleStatus(ScheduleId scheduleId) throws NotFoundException {
    return null;
  }

  @Override
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return null;
  }

  @Override
  public List<ProgramSchedule> listSchedules(ProgramId programId) {
    return null;
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId) throws NotFoundException {
    return null;
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId) throws NotFoundException {
    return null;
  }

  @Override
  public Collection<ProgramScheduleRecord> findSchedules(String triggerKey) {
    return null;
  }
}
