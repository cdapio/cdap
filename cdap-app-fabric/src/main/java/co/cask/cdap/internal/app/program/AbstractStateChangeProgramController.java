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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Abstract implementation of {@link AbstractProgramController} that responds to state transitions and persists all
 * state changes.
 */
public abstract class AbstractStateChangeProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStateChangeProgramController.class);
  private final ProgramId programId;
  private final RunId runId;
  private final String twillRunId;
  private final RuntimeStore runtimeStore;
  private final ProgramOptions options;

  public AbstractStateChangeProgramController(Service service, final ProgramId programId, final RunId runId,
                                              @Nullable String componentName, final String twillRunId,
                                              final RuntimeStore runtimeStore, final ProgramOptions options) {
    super(programId, runId, componentName);
    this.programId = programId;
    this.runId = runId;
    this.twillRunId = twillRunId;
    this.runtimeStore = runtimeStore;
    this.options = options;

    // Add listener to the service for Spark and MapReduce programs
    if (programId.getType() == ProgramType.MAPREDUCE || programId.getType() == ProgramType.SPARK) {
      service.addListener(
        new ServiceListenerAdapter() {
          @Override
          public void starting() {
            recordProgramStart();
          }

          @Override
          public void terminated(Service.State from) {
            ProgramRunStatus runStatus = ProgramController.State.COMPLETED.getRunStatus();
            if (from == Service.State.STOPPING) {
              // Service was killed
              runStatus = ProgramController.State.KILLED.getRunStatus();
            }

            recordProgramTerminated(runStatus);
          }

          @Override
          public void failed(Service.State from, @Nullable final Throwable failure) {
            recordProgramError(failure);
          }
        },
        Threads.SAME_THREAD_EXECUTOR
      );
    }
  }

  public AbstractStateChangeProgramController(final ProgramId programId, final RunId runId,
                                              @Nullable String componentName, final String twillRunId,
                                              final RuntimeStore runtimeStore, final ProgramOptions options) {
    super(programId, runId, componentName);
    this.programId = programId;
    this.runId = runId;
    this.twillRunId = twillRunId;
    this.runtimeStore = runtimeStore;
    this.options = options;

    addListener(
      createProgramListener(),
      Threads.SAME_THREAD_EXECUTOR
    );
  }

  private Listener createProgramListener() {
    return new AbstractListener() {
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
        recordProgramStart();
      }

      @Override
      public void completed() {
        LOG.debug("Program {} completed successfully.", programId);
        recordProgramTerminated(State.COMPLETED.getRunStatus());
      }

      @Override
      public void killed() {
        LOG.debug("Program {} killed.", programId);
        recordProgramTerminated(State.KILLED.getRunStatus());
      }

      @Override
      public void suspended() {
        LOG.debug("Suspending Program {} with run id {}.", programId, runId.getId());
        Retries.supplyWithRetries(new Supplier<Void>() {
          @Override
          public Void get() {
            runtimeStore.setSuspend(programId, runId.getId());
            return null;
          }
        }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
      }

      @Override
      public void resuming() {
        LOG.debug("Resuming Program {} {}.", programId, runId.getId());
        Retries.supplyWithRetries(new Supplier<Void>() {
          @Override
          public Void get() {
            runtimeStore.setResume(programId, runId.getId());
            return null;
          }
        }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
      }

      @Override
      public void error(final Throwable cause) {
        LOG.info("Program stopped with error {}, {}", programId, runId, cause);
        recordProgramError(cause);
      }
    };
  }

  private void recordProgramStart() {
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
        runtimeStore.setStart(programId, runId.getId(), finalStartTimeInSeconds, twillRunId,
                              options.getUserArguments().asMap(), options.getArguments().asMap());
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  private void recordProgramTerminated(final ProgramRunStatus runStatus) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setStop(programId, runId.getId(),
                             TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                             runStatus);
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }

  private void recordProgramError(final Throwable cause) {
    Retries.supplyWithRetries(new Supplier<Void>() {
      @Override
      public Void get() {
        runtimeStore.setStop(programId, runId.getId(),
                             TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                             ProgramController.State.ERROR.getRunStatus(), new BasicThrowable(cause));
        return null;
      }
    }, RetryStrategies.fixDelay(Constants.Retry.RUN_RECORD_UPDATE_RETRY_DELAY_SECS, TimeUnit.SECONDS));
  }
}
