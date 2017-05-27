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

package co.cask.cdap.internal.schedule;

import co.cask.cdap.api.app.ProgramType;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.internal.schedule.trigger.Trigger;

import java.util.List;
import java.util.Map;

/**
 * Information for creating a program schedule.
 * See {@link co.cask.cdap.api.app.AbstractApplication#buildSchedule(String, ProgramType, String)}
 * for how to build an instance of this object.
 */
public class ScheduleCreationSpec {

  private final String name;
  private final String description;
  private final String programName;
  private final Map<String, String> properties;
  private final Trigger trigger;
  private final List<? extends Constraint> constraints;
  private final long timeoutMillis;

  public ScheduleCreationSpec(String name, String description, String programName, Map<String, String> properties,
                              Trigger trigger, List<? extends Constraint> constraints, long timeoutMillis) {
    this.name = name;
    this.description = description;
    this.programName = programName;
    this.properties = properties;
    this.trigger = trigger;
    this.constraints = constraints;
    this.timeoutMillis = timeoutMillis;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getProgramName() {
    return programName;
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

  public long getTimeoutMillis() {
    return timeoutMillis;
  }
}
