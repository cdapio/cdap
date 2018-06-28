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

import co.cask.cdap.api.schedule.Trigger;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A getBuilder for {@link ProgramSchedule}
 */
public class ProgramScheduleBuilder {
  private String description;
  private ProgramId programId;
  private ScheduleId scheduleId;
  private Map<String, String> properties;
  private Trigger trigger;
  private List<? extends Constraint> constraints;
  private long timeoutMillis;

  public ProgramScheduleBuilder(ProgramSchedule schedule) {
    this.description = schedule.getDescription();
    this.programId = schedule.getProgramId();
    this.scheduleId = schedule.getScheduleId();
    this.properties = new HashMap<>(schedule.getProperties());
    this.trigger = schedule.getTrigger();
    this.constraints = new ArrayList<>(schedule.getConstraints());
    this.timeoutMillis = schedule.getTimeoutMillis();
  }

  public ProgramSchedule build() {
    return new ProgramSchedule(scheduleId.getSchedule(), description, programId, properties, trigger,
                               constraints, timeoutMillis);
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setProgramId(ProgramId programId) {
    this.programId = programId;
  }

  public void setScheduleId(ScheduleId scheduleId) {
    this.scheduleId = scheduleId;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public void setTrigger(Trigger trigger) {
    this.trigger = trigger;
  }

  public void setConstraints(List<? extends Constraint> constraints) {
    this.constraints = constraints;
  }

  public void setTimeoutMillis(long timeoutMillis) {
    this.timeoutMillis = timeoutMillis;
  }

  /**
   * Updates the properties with the key-value pair or insert the new key-value pair in the properties.
   *
   * @param key the key of the property to update or insert
   * @param value the new value of the property
   */
  public void updateProperties(String key, String value) {
    if (properties == null) {
      properties = new HashMap<>();
    }
    properties.put(key, value);
  }
}
