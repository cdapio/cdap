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

import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;

import javax.annotation.Nullable;

/**
 * A no-op implementation of {@link ProgramStateWriter}
 */
public final class NoOpProgramStateWriter implements ProgramStateWriter {
  @Override
  public void start(long startTime) {

  }

  @Override
  public void running(long startTime) {

  }

  @Override
  public void stop(long endTime, ProgramRunStatus runStatus, @Nullable BasicThrowable cause) {

  }

  @Override
  public void suspend() {

  }

  @Override
  public void resume() {

  }
}
