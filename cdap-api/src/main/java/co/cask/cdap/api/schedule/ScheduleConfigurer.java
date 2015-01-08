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

import java.util.HashSet;
import java.util.Set;

/**
 * Configures the {@link Schedule}.
 */
public final class ScheduleConfigurer {
  private final Schedule schedule;
  private final Set<ScheduleProgramInfo> programs;

  public ScheduleConfigurer(Schedule schedule) {
    this.schedule = schedule;
    this.programs = new HashSet<ScheduleProgramInfo>();
  }

  /**
   * Add a program to the {@link Schedule}
   * @param scheduleProgramInfo the program name and program type information
   */
  public void addProgram(ScheduleProgramInfo scheduleProgramInfo) {
    this.programs.add(scheduleProgramInfo);
  }

  public ScheduleSpecification createSpecification() {
    return new ScheduleSpecification(schedule, programs);
  }
}
