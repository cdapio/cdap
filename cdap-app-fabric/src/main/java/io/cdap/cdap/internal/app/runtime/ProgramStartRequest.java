/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;

/**
 * Request object for starting a new Program run.
 */
public class ProgramStartRequest {

  private final ProgramOptions programOptions;
  private final ProgramDescriptor programDescriptor;
  private final ProgramRunId programRunId;
  private final RunId runId;

  public ProgramStartRequest(ProgramOptions programOptions,
      ProgramDescriptor programDescriptor,
      ProgramRunId programRunId) {
    this.programOptions = programOptions;
    this.programDescriptor = programDescriptor;
    this.programRunId = programRunId;
    this.runId = RunIds.fromString(programRunId.getRun());
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public RunId getRunId() {
    return runId;
  }
}
