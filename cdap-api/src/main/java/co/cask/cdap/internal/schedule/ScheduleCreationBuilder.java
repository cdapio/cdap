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
import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;

import java.util.List;
import java.util.Map;

/**
 * Builder for creating a ScheduleCreationSpec.
 *
 * See {@link co.cask.cdap.api.app.AbstractApplication#buildSchedule(String, ProgramType, String)}
 * for how to build an instance of this object.
 */
public class ScheduleCreationBuilder {

  protected final String name;
  protected final String description;
  protected final String programName;
  protected final Map<String, String> properties;
  protected final List<? extends Constraint> constraints;
  protected final long timeoutMillis;
  protected final TriggerBuilder triggerBuilder;

  public ScheduleCreationBuilder(String name, String description, String programName, Map<String, String> properties,
                                 List<? extends Constraint> constraints, long timeoutMillis,
                                 TriggerBuilder triggerBuilder) {
    this.name = name;
    this.description = description;
    this.programName = programName;
    this.properties = properties;
    this.constraints = constraints;
    this.timeoutMillis = timeoutMillis;
    this.triggerBuilder = triggerBuilder;
  }

  public String getName() {
    return name;
  }

  public ScheduleCreationSpec build(String namespace, String applicationName, String applicationVersion) {
    return new ScheduleCreationSpec(name, description, programName, properties,
                                    triggerBuilder.build(namespace, applicationName, applicationVersion),
                                    constraints, timeoutMillis);
  }
}
