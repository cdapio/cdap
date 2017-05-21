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

package co.cask.cdap.proto;

import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a schedule in a REST request/response.
 *
 * All fields are nullable because after Json deserialization, they may be null. Also, this is used both
 * for creating a schedule and for updating a schedule. When updating, all fields are optional - only the
 * fields that are present will be updated.
 */
public class ScheduleDetail {

  private final String name;
  private final String description;
  private final ScheduleProgramInfo program;
  private final Map<String, String> properties;
  private final Trigger trigger;
  private final List<Constraint> constraints;

  public ScheduleDetail(@Nullable String name,
                        @Nullable String description,
                        @Nullable ScheduleProgramInfo program,
                        @Nullable Map<String, String> properties,
                        @Nullable Trigger trigger,
                        @Nullable List<Constraint> constraints) {
    this.name = name;
    this.description = description;
    this.program = program;
    this.properties = properties;
    this.trigger = trigger;
    this.constraints = constraints;
  }

  @Nullable
  public String getName() {
    return name;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nullable
  public ScheduleProgramInfo getProgram() {
    return program;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return properties;
  }

  @Nullable
  public Trigger getTrigger() {
    return trigger;
  }

  @Nullable
  public List<Constraint> getConstraints() {
    return constraints;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScheduleDetail that = (ScheduleDetail) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(program, that.program) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(trigger, that.trigger) &&
      Objects.equals(constraints, that.constraints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, program, properties, trigger, constraints);
  }
}
