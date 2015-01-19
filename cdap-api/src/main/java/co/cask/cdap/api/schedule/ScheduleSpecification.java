/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.schedule;

import co.cask.cdap.api.workflow.ScheduleProgramInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Specification for {@link Schedule}.
 */
public final class ScheduleSpecification {
  private final Schedule schedule;
  private final ScheduleProgramInfo program;
  private final Map<String, String> properties;

  public ScheduleSpecification(Schedule schedule, ScheduleProgramInfo program, Map<String, String> properties) {
    this.schedule = schedule;
    this.program = program;
    this.properties = properties == null ? new HashMap<String, String>() :
      Collections.unmodifiableMap(new HashMap<String, String>(properties));
  }

  /**
   * @return the program associated with {@link ScheduleSpecification}
   */
  public ScheduleProgramInfo getProgram() {
    return program;
  }

  /**
   * @return the {@link Schedule} associated with {@link ScheduleSpecification}
   */
  public Schedule getSchedule() {
    return schedule;
  }

  /**
   * @return the properties associated with the schedule
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
