/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A base implementation of {@link ApplicationManager}.
 */
public abstract class AbstractApplicationManager implements ApplicationManager {
  protected final ApplicationId application;

  public AbstractApplicationManager(Id.Application application) {
    this.application = application.toEntityId();
  }

  public AbstractApplicationManager(ApplicationId application) {
    this.application = application;
  }

  @Override
  public void startProgram(Id.Program programId) {
    startProgram(programId.toEntityId());
  }

  @Override
  public void startProgram(ProgramId programId) {
    startProgram(programId, ImmutableMap.<String, String>of());
  }

  @Override
  public void startProgram(Id.Program programId, Map<String, String> arguments) {
    startProgram(programId.toEntityId(), arguments);
  }

  @Override
  public void stopProgram(Id.Program programId) {
    stopProgram(programId.toEntityId());
  }

  @Override
  public boolean isRunning(Id.Program programId) {
    return isRunning(programId.toEntityId());
  }

  @Override
  public List<RunRecord> getHistory(Id.Program programId, ProgramRunStatus status) {
    return getHistory(programId.toEntityId(), status);
  }

  @Override
  public void waitForStopped(final ProgramId programId) throws Exception {
    // TODO CDAP-12362 This should be exposed to ProgramManager to stop all runs of a program
    // Ensure that there are no pending run records before moving on to the next test.
    Tasks.waitFor(true, () -> isStopped(programId), 120, TimeUnit.SECONDS);
  }
}
