/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.scheduler;

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleMeta;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleRecord;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

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
  public void modifySchedulesTriggeredByDeletedProgram(ProgramId programId) {

  }

  @Override
  public ProgramSchedule getSchedule(ScheduleId scheduleId) throws NotFoundException {
    return null;
  }

  @Override
  public ProgramScheduleMeta getScheduleMetadata(ScheduleId scheduleId) throws NotFoundException {
    return null;
  }

  @Override
  public ProgramScheduleStatus getScheduleStatus(ScheduleId scheduleId) throws NotFoundException {
    return null;
  }

  @Override
  public List<ProgramSchedule> listSchedules(ApplicationId appId) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public List<ProgramSchedule> listSchedules(ProgramId programId) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public List<ProgramSchedule> listSchedules(NamespaceId namespaceId,
                                             Predicate<ProgramSchedule> filter) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ApplicationId appId) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public List<ProgramScheduleRecord> listScheduleRecords(ProgramId programId) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public Collection<ProgramScheduleRecord> findSchedules(String triggerKey) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void reEnableSchedules(NamespaceId namesapceId, long startTime, long endTime) {

  }
}
