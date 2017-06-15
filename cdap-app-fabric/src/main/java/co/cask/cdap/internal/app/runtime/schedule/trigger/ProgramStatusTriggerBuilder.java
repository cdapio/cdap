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
import co.cask.cdap.internal.schedule.trigger.TriggerBuilder;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A Trigger builder that builds a ProgramStatusTrigger.
 */
public class ProgramStatusTriggerBuilder implements TriggerBuilder {
  private String programNamespace;
  private String programApplication;
  private String programApplicationVersion;
  private ProgramType programType;
  private String programName;
  private Set<ProgramStatus> programStatus;

  public ProgramStatusTriggerBuilder(@Nullable String programNamespace, @Nullable String programApplication,
                                     @Nullable String programApplicationVersion, String programType,
                                     String programName, ProgramStatus... programStatus) {
    this.programNamespace = programNamespace;
    this.programApplication = programApplication;
    this.programApplicationVersion = programApplicationVersion;
    this.programType = ProgramType.valueOf(programType);
    this.programName = programName;
    this.programStatus = new HashSet<>(Arrays.asList(programStatus));
  }

  @Override
  public ProgramStatusTrigger build(String namespace, String applicationName, String applicationVersion) {
    // Inherit environment attributes from the deployed application
    if (programNamespace == null) {
      programNamespace = namespace;
    }
    if (programApplication == null) {
      programApplication = applicationName;
    }
    if (programApplicationVersion == null) {
      programApplicationVersion = applicationVersion;
    }

    ProgramId programId = new ApplicationId(programNamespace, programApplication,
                                            programApplicationVersion).program(programType, programName);
    return new ProgramStatusTrigger(programId, programStatus);
  }
}
