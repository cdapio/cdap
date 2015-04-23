/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Objects;
import org.apache.twill.api.RunId;

import javax.annotation.Nullable;

/**
 *
 */
public final class SimpleRuntimeInfo implements ProgramRuntimeService.RuntimeInfo {

  private final ProgramController controller;
  private final Id.Program programId;
  private final RunId twillRunId;

  public SimpleRuntimeInfo(ProgramController controller, Program program) {
    this(controller, program, null);
  }

  public SimpleRuntimeInfo(ProgramController controller, Program program, @Nullable RunId twillRunId) {
    this(controller, Id.Program.from(program.getNamespaceId(), program.getApplicationId(),
                                     program.getType(), program.getName()), twillRunId);

  }

  public SimpleRuntimeInfo(ProgramController controller, Id.Program programId, @Nullable RunId twillRunId) {
    this.controller = controller;
    this.programId = programId;
    this.twillRunId = twillRunId;
  }

  @Override
  public ProgramController getController() {
    return controller;
  }

  @Override
  public ProgramType getType() {
    return programId.getType();
  }

  @Override
  public Id.Program getProgramId() {
    return programId;
  }

  @Nullable
  @Override
  public RunId getTwillRunId() {
    return twillRunId;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(ProgramRuntimeService.RuntimeInfo.class)
      .add("type", programId.getType())
      .add("appId", programId.getApplicationId())
      .add("programId", programId.getId())
      .toString();
  }
}
