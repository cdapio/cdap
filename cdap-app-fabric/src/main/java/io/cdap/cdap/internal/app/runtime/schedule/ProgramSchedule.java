/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.base.Objects;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.internal.app.runtime.schedule.store.Schedulers;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.List;
import java.util.Map;

/**
 * A schedule for a program.
 */
public class ProgramSchedule {

  private final String description;
  private final ProgramReference programRef;
  private final ScheduleId scheduleId;
  private final Map<String, String> properties;
  private final Trigger trigger;
  private final List<? extends Constraint> constraints;
  private final long timeoutMillis;

  public ProgramSchedule(String name, String description,
                         ProgramReference programReference, Map<String, String> properties,
                         Trigger trigger, List<? extends Constraint> constraints) {
    this(name, description, programReference, properties, trigger, constraints, Schedulers.JOB_QUEUE_TIMEOUT_MILLIS);
  }

  public ProgramSchedule(String name, String description, ProgramReference programReference,
                         Map<String, String> properties, Trigger trigger, List<? extends Constraint> constraints,
                         long timeoutMillis) {
    this.description = description;
    this.programRef = programReference;
    this.scheduleId = new ScheduleId(programRef.getNamespace(), programRef.getApplication(), name);
    this.properties = properties;
    this.trigger = trigger;
    this.constraints = constraints;
    this.timeoutMillis = timeoutMillis;
  }

  public String getName() {
    return scheduleId.getSchedule();
  }

  public String getDescription() {
    return description;
  }

  public ProgramReference getProgramReference() {
    return programRef;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public List<? extends Constraint> getConstraints() {
    return constraints;
  }

  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
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
      Objects.equal(this.programRef, that.programRef) &&
      Objects.equal(this.description, that.description) &&
      Objects.equal(this.properties, that.properties) &&
      Objects.equal(this.trigger, that.trigger) &&
      Objects.equal(this.constraints, that.constraints) &&
      Objects.equal(this.timeoutMillis, that.timeoutMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(scheduleId, programRef, description, properties, trigger, constraints, timeoutMillis);
  }

  @Override
  public String toString() {
    return "ProgramSchedule{" +
      "scheduleId=" + scheduleId +
      ", programRef=" + programRef +
      ", description='" + description + '\'' +
      ", properties=" + properties +
      ", trigger=" + trigger +
      ", constraints=" + constraints +
      ", timeoutMillis=" + timeoutMillis +
      '}';
  }

  public ScheduleDetail toScheduleDetail() {
    ScheduleProgramInfo programInfo =
      new ScheduleProgramInfo(programRef.getType().getSchedulableType(), programRef.getProgram());
    return new ScheduleDetail(scheduleId.getNamespace(), scheduleId.getApplication(), scheduleId.getSchedule(),
                              description, programInfo, properties, trigger, constraints, timeoutMillis,
                              null, null);
  }

  public static ProgramSchedule fromScheduleDetail(ScheduleDetail schedule) throws IllegalArgumentException {
    ProgramType programType = ProgramType.valueOfSchedulableType(schedule.getProgram().getProgramType());
    ProgramReference programReference = new ProgramReference(
      schedule.getNamespace(), schedule.getApplication(), programType, schedule.getProgram().getProgramName());
    ProgramSchedule programSchedule = new ProgramSchedule(
      schedule.getName(), schedule.getDescription(), programReference, schedule.getProperties(),
      schedule.getTrigger(), schedule.getConstraints(), schedule.getTimeoutMillis());
    return programSchedule;
  }
}

