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

package io.cdap.cdap.internal.app.runtime.service;

import com.google.common.base.Objects;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;

import javax.annotation.Nullable;

/**
 *
 */
public final class SimpleRuntimeInfo implements ProgramRuntimeService.RuntimeInfo {

  private final ProgramController controller;
  private final ProgramId programId;
  private volatile RunId twillRunId;

  public SimpleRuntimeInfo(ProgramController controller, ProgramId programId) {
    this(controller, programId, null);
  }

  public SimpleRuntimeInfo(ProgramController controller, ProgramId programId, @Nullable RunId twillRunId) {
    this.controller = controller;
    this.programId = programId;
    this.twillRunId = twillRunId;
  }

  public void setTwillRunId(RunId twillRunId) {
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
  public ProgramId getProgramId() {
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
      .add("programId", programId)
      .add("twillRunId", twillRunId)
      .toString();
  }
}
