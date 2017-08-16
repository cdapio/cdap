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


package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;

import java.util.EnumSet;
import java.util.Map;

/**
 * A Trigger builder that builds a {@link ProgramStatusTrigger}.
 */
public class ProgramStatusTriggerBuilder implements TriggerBuilder {
  private final ProgramType programType;
  private final String programName;
  private final EnumSet<ProgramStatus> programStatuses;
  private final Map<String, String> runtimeArgs;

  public ProgramStatusTriggerBuilder(String programType, String programName, ProgramStatus... programStatuses) {
    this(programType, programName, ImmutableMap.<String, String>of(), programStatuses);
  }

  public ProgramStatusTriggerBuilder(String programType, String programName, Map<String, String> runtimeArgs,
                                     ProgramStatus... programStatuses) {
    this.programType = ProgramType.valueOf(programType);
    this.programName = programName;
    this.runtimeArgs = runtimeArgs;

    // User can not specify any program statuses, or specify null, which is an array of length 1 containing null
    if (programStatuses.length == 0 || (programStatuses.length == 1 && programStatuses[0] == null)) {
      throw new IllegalArgumentException("Must set a program state for the triggering program");
    }
    this.programStatuses = EnumSet.of(programStatuses[0], programStatuses);
  }

  @Override
  public ProgramStatusTrigger build(String namespace, String applicationName, String applicationVersion) {
    // Inherit environment attributes from the deployed application
    ProgramId programId = new ApplicationId(namespace, applicationName, applicationVersion).program(programType,
                                                                                                    programName);
    return new ProgramStatusTrigger(programId, programStatuses, runtimeArgs);
  }
}
