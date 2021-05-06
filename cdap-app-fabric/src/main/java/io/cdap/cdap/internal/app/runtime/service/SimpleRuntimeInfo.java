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

import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;

import java.io.Closeable;
import javax.annotation.Nullable;

/**
 * A straightforward implementation of the {@link ProgramRuntimeService.RuntimeInfo} interface that allows
 * setting of the twill run id.
 */
public final class SimpleRuntimeInfo implements ProgramRuntimeService.RuntimeInfo, Closeable {

  private final ProgramController controller;
  private final ProgramId programId;
  private final Runnable cleanupTask;
  private volatile RunId twillRunId;

  public SimpleRuntimeInfo(ProgramController controller, ProgramId programId) {
    this(controller, programId, () -> { });
  }

  public SimpleRuntimeInfo(ProgramController controller, ProgramId programId, Runnable cleanupTask) {
    this.controller = controller;
    this.programId = programId;
    this.cleanupTask = cleanupTask;
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
  public void close() {
    cleanupTask.run();
  }
}
