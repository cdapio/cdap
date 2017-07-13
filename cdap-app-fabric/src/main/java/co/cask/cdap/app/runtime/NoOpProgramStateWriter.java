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

package co.cask.cdap.app.runtime;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A no-op {@link ProgramStateWriter}
 */
public final class NoOpProgramStateWriter implements ProgramStateWriter {
  @Override
  public void start(ProgramRunId programRunId, ProgramOptions programOptions, @Nullable String twillRunId) {
    // no-op
  }

  @Override
  public void running(ProgramRunId programRunId, @Nullable String twillRunId) {
    // no-op
  }

  @Override
  public void completed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    // no-op
  }

  @Override
  public void killed(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken) {
    // no-op
  }

  @Override
  public void error(ProgramRunId programRunId, @Nullable WorkflowToken workflowToken, Throwable failureCause) {
    // no-op
  }

  @Override
  public void suspend(ProgramRunId programRunId) {
    // no-op
  }

  @Override
  public void resume(ProgramRunId programRunId) {
    // no-op
  }
}
