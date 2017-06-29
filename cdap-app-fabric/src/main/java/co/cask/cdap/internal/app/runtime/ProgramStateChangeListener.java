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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramEventPublisher;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Listener that persists program status changes
 */
public class ProgramStateChangeListener extends AbstractListener {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramStateChangeListener.class);

  private ProgramEventPublisher programEventPublisher;
  private ProgramId programId;
  private RunId runId;
  private String twillRunId;
  private Arguments userArguments;
  private Arguments systemArguments;
  private final SettableFuture<ProgramController.State> state;
  private WorkflowToken workflowToken;

  public ProgramStateChangeListener(ProgramEventPublisher programEventPublisher, @Nullable String twillRunId,
                                    Arguments userArguments, Arguments systemArguments,
                                    @Nullable WorkflowToken workflowToken) {
    this.programEventPublisher = programEventPublisher;
    this.programId = programEventPublisher.getProgramId();
    this.runId = programEventPublisher.getRunId();
    this.twillRunId = twillRunId;
    this.userArguments = userArguments;
    this.systemArguments = systemArguments;
    this.state = SettableFuture.create();
    this.workflowToken = workflowToken;
  }

  @Override
  public void init(ProgramController.State state, @Nullable Throwable cause) {
    if (state == ProgramController.State.ALIVE) {
      alive();
    } else if (state == ProgramController.State.COMPLETED) {
      completed();
    } else if (state == ProgramController.State.ERROR) {
      error(cause);
    } else {
      super.init(state, cause);
    }
  }

  @Override
  public void alive() {
    // Get start time from RunId
    long startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
    if (startTimeInSeconds == -1) {
      // If RunId is not time-based, use current time as start time
      startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    }

    final long finalStartTimeInSeconds = startTimeInSeconds;
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.running(finalStartTimeInSeconds, twillRunId, userArguments, systemArguments);
        return null;
      }
    }, RetryStrategies.exponentialDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, 5000, TimeUnit.SECONDS));
  }

  @Override
  public void completed() {
    LOG.debug("Program {} completed successfully.", programId);
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.stop(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.COMPLETED.getRunStatus(), workflowToken, null);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
    state.set(ProgramController.State.COMPLETED);
  }

  @Override
  public void killed() {
    LOG.debug("Program {} killed.", programId);
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.stop(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.COMPLETED.getRunStatus(), workflowToken, null);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
    state.set(ProgramController.State.KILLED);
  }

  @Override
  public void suspended() {
    LOG.debug("Suspending Program {} with run id {}.", programId, runId.getId());
    state.set(ProgramController.State.SUSPENDING);
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.suspend();
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
    state.set(ProgramController.State.SUSPENDED);
  }

  @Override
  public void resuming() {
    LOG.debug("Resuming Program {} {}.", programId, runId.getId());
    state.set(ProgramController.State.RESUMING);
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.resume();
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
    state.set(ProgramController.State.ALIVE);
  }

  @Override
  public void error(final Throwable cause) {
    LOG.info("Program stopped with error {}, {}", programId, runId, cause);
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        programEventPublisher.stop(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                   ProgramController.State.ERROR.getRunStatus(), workflowToken, null);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
    state.setException(cause);
  }

  public SettableFuture<ProgramController.State> getState() {
    return state;
  }
}
