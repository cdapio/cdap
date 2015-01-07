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

import co.cask.cdap.api.workflow.ProgramNameTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Specification for {@link Schedule}.
 */
public class ScheduleSpecification {
  private final Schedule schedule;
  private final List<ProgramNameTypeInfo> programList;

  public ScheduleSpecification(Schedule schedule) {
    this.schedule = schedule;
    this.programList = new ArrayList<ProgramNameTypeInfo>();
  }

  /**
   * Add a program to the schedule associated with {@link ScheduleSpecification}
   * @param programNameTypeInfo the program name and program type information
   */
  public void addProgram(ProgramNameTypeInfo programNameTypeInfo) {
    this.programList.add(programNameTypeInfo);
  }

  /**
   * @return the {@link List} of programs associated with {@link ScheduleSpecification}
   */
  public List<ProgramNameTypeInfo> getProgramList() {
    return programList;
  }

  /**
   * @return the {@link Schedule} associated with {@link ScheduleSpecification}
   */
  public Schedule getSchedule() {
    return schedule;
  }
}
