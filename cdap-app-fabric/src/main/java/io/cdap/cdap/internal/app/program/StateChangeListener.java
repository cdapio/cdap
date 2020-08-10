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

package io.cdap.cdap.internal.app.program;

import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.lang.Delegator;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.distributed.AbstractTwillProgramController;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A listener that responds to state transitions and persists all state changes using a provided
 * {@link ProgramStateWriter}.
 */
public class StateChangeListener extends AbstractListener {
  private static final Logger LOG = LoggerFactory.getLogger(StateChangeListener.class);

  private final ProgramRunId programRunId;
  private final ProgramController programController;
  private final ProgramStateWriter programStateWriter;

  public StateChangeListener(ProgramController programController, ProgramStateWriter programStateWriter) {
    this.programRunId = programController.getProgramRunId();
    this.programController = programController;
    this.programStateWriter = programStateWriter;
  }

  @Override
  public void init(ProgramController.State currentState, @Nullable Throwable cause) {
    switch (currentState) {
      case ALIVE:
        alive();
        break;
      case SUSPENDED:
        suspended();
        break;
      case RESUMING:
        resuming();
        break;
      case COMPLETED:
        completed();
        break;
      case KILLED:
        killed();
        break;
      case ERROR:
        error(cause);
        break;
    }
  }

  @Override
  public void alive() {
    LOG.trace("Program {} is alive.", programRunId);
    programStateWriter.running(programRunId, getTwillRunId());
  }

  @Override
  public void completed() {
    LOG.trace("Program {} completed successfully.", programRunId);
    programStateWriter.completed(programRunId);
  }

  @Override
  public void killed() {
    LOG.trace("Program {} killed.", programRunId);
    programStateWriter.killed(programRunId);
  }

  @Override
  public void suspended() {
    LOG.trace("Suspending Program {} .", programRunId);
    programStateWriter.suspend(programRunId);
  }

  @Override
  public void resuming() {
    LOG.trace("Resuming Program {}.", programRunId);
    programStateWriter.resume(programRunId);
  }

  @Override
  public void error(Throwable cause) {
    LOG.trace("Program {} stopped with error: {}", programRunId, cause);
    programStateWriter.error(programRunId, cause);
  }

  @Nullable
  private String getTwillRunId() {
    ProgramController programController = this.programController;
    while (programController instanceof Delegator) {
      //noinspection unchecked
      programController = ((Delegator<ProgramController>) programController).getDelegate();
    }
    if (programController instanceof AbstractTwillProgramController) {
      return ((AbstractTwillProgramController) programController).getTwillRunId().getId();
    }
    return null;
  }
}
