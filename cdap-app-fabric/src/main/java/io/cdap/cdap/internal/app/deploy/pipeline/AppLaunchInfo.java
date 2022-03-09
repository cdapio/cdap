/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import org.apache.twill.api.RunId;

public class AppLaunchInfo {

  private final ProgramDescriptor programDescriptor;
  private final RunId runId;
  private final ProgramOptions programOptions;

  private final boolean isDistributed;

  public AppLaunchInfo(ProgramDescriptor programDescriptor, ProgramOptions programOptions,
      RunId runId, boolean isDistributed) {
    this.programDescriptor = programDescriptor;
    this.programOptions = programOptions;
    this.runId = runId;
    this.isDistributed = isDistributed;
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public RunId getRunId() {
    return runId;
  }

  public boolean isDistributed() {
    return isDistributed;
  }
}
