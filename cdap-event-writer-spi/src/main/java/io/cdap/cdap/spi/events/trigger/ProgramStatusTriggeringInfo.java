/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.events.trigger;

import java.util.Map;
import java.util.Objects;

/**
 * Represents the properties of a Program Status based Trigger.
 */
public class ProgramStatusTriggeringInfo implements TriggeringInfo {
  private final Type type;
  private final ScheduleId scheduleId;
  private final String runID;
  private final String programName;
  private final String applicationName;
  private final String namespace;
  private final Map<String, String> runtimeArgs;

  public ProgramStatusTriggeringInfo(ScheduleId scheduleId, String runID, String programName,
                                     String applicationName, String namespace, Map<String, String> runtimeArgs) {
    this.type = Type.PROGRAM_STATUS;
    this.scheduleId = scheduleId;
    this.runID = runID;
    this.programName = programName;
    this.applicationName = applicationName;
    this.namespace = namespace;
    this.runtimeArgs = runtimeArgs;
  }

  /**
   * @return The type of the trigger
   */
  @Override
  public Type getType() {
    return type;
  }

  /**
   * @return The schedule ID of the program status trigger that triggered the current run
   */
  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  /**
   * @return The run ID of the triggering pipeline
   */
  public String getRunID() {
    return runID;
  }

  /**
   * @return The program name of the triggering pipeline
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * @return The application name of the triggering pipeline
   */
  public String getApplicationName() {
    return applicationName;
  }

  /**
   * @return The namespace of the triggering pipeline
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return Runtime arguments of the triggering pipeline run
   */
  public Map<String, String> getRuntimeArgs() {
    return runtimeArgs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProgramStatusTriggeringInfo)) {
      return false;
    }
    ProgramStatusTriggeringInfo that = (ProgramStatusTriggeringInfo) o;
    return getType() == that.getType()
      && Objects.equals(getScheduleId(), that.getScheduleId())
      && Objects.equals(getRunID(), that.getRunID())
      && Objects.equals(getProgramName(), that.getProgramName())
      && Objects.equals(getApplicationName(), that.getApplicationName())
      && Objects.equals(getNamespace(), that.getNamespace())
      && Objects.equals(getRuntimeArgs(), that.getRuntimeArgs());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getScheduleId(), getRunID(), getProgramName(),
                        getApplicationName(), getNamespace(), getRuntimeArgs());
  }
}
