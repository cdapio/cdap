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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;

/**
 * Represents name and type information of the program.
 */
public final class ScheduleProgramInfo {
  private final String programName;
  private final SchedulableProgramType programType;

  public ScheduleProgramInfo(SchedulableProgramType programType, String programName) {
    this.programType = programType;
    this.programName = programName;
  }

  /**
   *
   * @return name of the {@link SchedulableProgramType}
   */
  public String getProgramName() {
    return programName;
  }

  /**
   *
   * @return type of the {@link SchedulableProgramType}
   */
  public SchedulableProgramType getProgramType() {
    return programType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ScheduleProgramInfo that = (ScheduleProgramInfo) o;

    return programName.equals(that.programName) && programType == that.programType;
  }

  @Override
  public int hashCode() {
    int result = programName.hashCode();
    result = 31 * result + programType.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ScheduleProgramInfo{");
    sb.append("programName='").append(programName).append('\'');
    sb.append(", programType=").append(programType);
    sb.append('}');
    return sb.toString();
  }
}
