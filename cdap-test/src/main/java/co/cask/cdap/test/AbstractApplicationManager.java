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

package co.cask.cdap.test;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

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

}
