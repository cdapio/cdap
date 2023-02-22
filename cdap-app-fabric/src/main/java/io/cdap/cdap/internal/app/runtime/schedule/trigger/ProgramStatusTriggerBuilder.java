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


package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramReference;

import java.util.EnumSet;

/**
 * A Trigger builder that builds a {@link ProgramStatusTrigger}.
 */
public class ProgramStatusTriggerBuilder implements TriggerBuilder {
  private final ProgramType programType;
  private final String programName;
  private final EnumSet<ProgramStatus> programStatuses;

  public ProgramStatusTriggerBuilder(String programType, String programName, ProgramStatus... programStatuses) {
    this.programType = ProgramType.valueOf(programType);
    this.programName = programName;

    // User can not specify any program statuses, or specify null, which is an array of length 1 containing null
    if (programStatuses.length == 0 || (programStatuses.length == 1 && programStatuses[0] == null)) {
      throw new IllegalArgumentException("Must set a program state for the triggering program");
    }
    this.programStatuses = EnumSet.of(programStatuses[0], programStatuses);
  }

  @Override
  public Type getType() {
    return Type.PROGRAM_STATUS;
  }

  @Override
  public ProgramStatusTrigger build(String namespace, String applicationName, String applicationVersion) {
    // Inherit environment attributes from the deployed application
    ProgramReference programRef = new ProgramReference(namespace, applicationName, programType, programName);
    return new ProgramStatusTrigger(programRef, programStatuses);
  }
}
