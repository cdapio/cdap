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
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.ScheduleDetail;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.common.base.Objects;

import java.util.List;
import java.util.Map;

/**
 * A schedule for a program.
 */
public class ProgramSchedule {
  private final String description;
  private final ProgramId programId;
  private final ScheduleId scheduleId;
  private final Map<String, String> properties;
  private final Trigger trigger;
  private final List<Constraint> constraints;

  public ProgramSchedule(String name, String description,
                         ProgramId programId, Map<String, String> properties,
                         Trigger trigger, List<Constraint> constraints) {
    this.description = description;
    this.programId = programId;
    this.scheduleId = programId.getParent().schedule(name);
    this.properties = properties;
    this.trigger = trigger;
    this.constraints = constraints;
  }

  public String getName() {
    return scheduleId.getSchedule();
  }

  public String getDescription() {
    return description;
  }

  public ProgramId getProgramId() {
    return programId;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public List<Constraint> getConstraints() {
    return constraints;
  }

  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramSchedule that = (ProgramSchedule) o;

    return Objects.equal(this.scheduleId, that.scheduleId) &&
      Objects.equal(this.programId, that.programId) &&
      Objects.equal(this.description, that.description) &&
      Objects.equal(this.properties, that.properties) &&
      Objects.equal(this.trigger, that.trigger) &&
      Objects.equal(this.constraints, that.constraints);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scheduleId, programId, description, properties, trigger, constraints);
  }

  @Override
  public String toString() {
    return "ProgramSchedule{" +
      "scheduleId=" + scheduleId +
      ", programId=" + programId +
      ", description='" + description + '\'' +
      ", properties=" + properties +
      ", trigger=" + trigger +
      ", constraints=" + constraints +
      '}';
  }

  public ScheduleDetail toScheduleDetail() {
    ScheduleProgramInfo programInfo =
      new ScheduleProgramInfo(programId.getType().getSchedulableType(), programId.getProgram());
    return new ScheduleDetail(scheduleId.getSchedule(), description, programInfo, properties, trigger, constraints);
  }
}

