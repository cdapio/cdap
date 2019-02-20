/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.proto.id.ProgramRunId;

import javax.annotation.Nullable;

/**
 * The result of stopping a program. The failure is null if the stop succeeded and non-null if the stop failed.
 */
public class ProgramStopResult {
  private final ProgramRunId programRunId;
  private final Throwable failure;

  public ProgramStopResult(ProgramRunId programRunId, @Nullable Throwable failure) {
    this.programRunId = programRunId;
    this.failure = failure;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  @Nullable
  public Throwable getFailure() {
    return failure;
  }
}
