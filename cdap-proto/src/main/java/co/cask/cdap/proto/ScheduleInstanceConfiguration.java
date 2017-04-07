/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import co.cask.cdap.api.schedule.Schedule;

import java.util.Collections;
import java.util.Map;

/**
 * POJO that carries schedule type and properties information for create schedule request
 */
public final class ScheduleInstanceConfiguration {
  private final String scheduleType;
  private final Schedule schedule;
  private final Map<String, String> program;
  private final Map<String, String> properties;

  public ScheduleInstanceConfiguration(String scheduleType, Schedule schedule,
                                       Map<String, String> program, Map<String, String> properties) {
    this.scheduleType = scheduleType;
    this.schedule = schedule;
    this.program = program;
    this.properties = properties;
  }

  public String getScheduleType() {
    return scheduleType;
  }

  public Map<String, String> getProperties() {
    return properties == null ? Collections.<String, String>emptyMap() : properties;
  }

  public Schedule getSchedule() {
    return schedule;
  }

  public Map<String, String> getProgram() {
    return program;
  }
}
