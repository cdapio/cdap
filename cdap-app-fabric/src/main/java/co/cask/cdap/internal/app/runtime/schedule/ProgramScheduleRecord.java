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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Objects;

/**
 * Represents all information for a schedule in the schedule store.
 */
public class ProgramScheduleRecord {
  private final ProgramSchedule schedule;
  private final ProgramScheduleMeta meta;

  public ProgramScheduleRecord(ProgramSchedule schedule, ProgramScheduleMeta meta) {
    this.schedule = schedule;
    this.meta = meta;
  }

  public ProgramSchedule getSchedule() {
    return schedule;
  }

  public ProgramScheduleMeta getMeta() {
    return meta;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramScheduleRecord that = (ProgramScheduleRecord) o;

    return Objects.equal(this.schedule, that.schedule) &&
           Objects.equal(this.meta, that.meta);

  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schedule, meta);
  }

  @Override
  public String toString() {
    return "ProgramScheduleRecord{" +
      "schedule=" + schedule +
      ", meta=" + meta +
      '}';
  }

  public ScheduleDetail toScheduleDetail() {
    ScheduleProgramInfo programInfo =
      new ScheduleProgramInfo(schedule.getProgramId().getType().getSchedulableType(),
                              schedule.getProgramId().getProgram());
    ScheduleId scheduleId = schedule.getScheduleId();
    return new ScheduleDetail(scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getVersion(),
                              scheduleId.getSchedule(), schedule.getDescription(), programInfo,
                              schedule.getProperties(), schedule.getTrigger(), schedule.getConstraints(),
                              schedule.getTimeoutMillis(), meta.getStatus().name());
  }
}
