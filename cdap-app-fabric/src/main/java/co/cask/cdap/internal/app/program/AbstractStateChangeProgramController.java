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
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
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

  public AbstractStateChangeProgramController(Service service, final ProgramId programId, final RunId runId,
                                              final ProgramStateWriter programStateWriter,
                                              @Nullable String componentName) {
    super(programId, runId, componentName);

    // Add listener to the service for Spark and MapReduce programs
    if (programId.getType() == ProgramType.MAPREDUCE || programId.getType() == ProgramType.SPARK) {
      service.addListener(
        new ServiceListenerAdapter() {
          @Override
          public void starting() {
            // Get start time from RunId
            long startTimeInSeconds = RunIds.getTime(runId, TimeUnit.SECONDS);
            if (startTimeInSeconds == -1) {
              // If RunId is not time-based, use current time as start time
              startTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            }
            programStateWriter.running(startTimeInSeconds);
          }

          @Override
          public void terminated(Service.State from) {
            ProgramRunStatus runStatus = ProgramController.State.COMPLETED.getRunStatus();
            if (from == Service.State.STOPPING) {
              // Service was killed
              runStatus = ProgramController.State.KILLED.getRunStatus();
            }
            long endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            programStateWriter.stop(endTimeInSeconds, runStatus, null);
          }

          @Override
          public void failed(Service.State from, @Nullable final Throwable failure) {
            long endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            programStateWriter.stop(endTimeInSeconds, State.ERROR.getRunStatus(),
                                    new BasicThrowable(failure));
          }
        },
        Threads.SAME_THREAD_EXECUTOR
      );
    }
  }

  public AbstractStateChangeProgramController(final ProgramId programId, final RunId runId,
                                              final ProgramStateWriter programStateWriter,
                                              @Nullable String componentName) {
    super(programId, runId, componentName);

    addListener(
      new AbstractListener() {
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
          programStateWriter.running(startTimeInSeconds);
        }

        @Override
        public void completed() {
          LOG.debug("Program {} completed successfully.", programId);
          long endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          programStateWriter.stop(endTimeInSeconds, State.COMPLETED.getRunStatus(), null);
        }

        @Override
        public void killed() {
          LOG.debug("Program {} killed.", programId);
          long endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          programStateWriter.stop(endTimeInSeconds, State.KILLED.getRunStatus(), null);
        }

        @Override
        public void suspended() {
          LOG.debug("Suspending Program {} with run id {}.", programId, runId.getId());
          programStateWriter.suspend();
        }

        @Override
        public void resuming() {
          LOG.debug("Resuming Program {} with run id {}.", programId, runId.getId());
          programStateWriter.resume();
        }

        @Override
        public void error(final Throwable cause) {
          LOG.info("Program stopped with error {}, {}: {}", programId, runId, cause);
          long endTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
          programStateWriter.stop(endTimeInSeconds, State.ERROR.getRunStatus(), new BasicThrowable(cause));
        }
      },
      Threads.SAME_THREAD_EXECUTOR
    );
  }
}
