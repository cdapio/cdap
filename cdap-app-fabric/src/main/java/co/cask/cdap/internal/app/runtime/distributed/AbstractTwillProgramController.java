/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.app.runtime.LogLevelUpdater;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * A {@link ProgramController} that control program through twill.
 */
public abstract class AbstractTwillProgramController extends AbstractProgramController
  implements ProgramController, LogLevelUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillProgramController.class);

  private final TwillController twillController;
  private volatile boolean stopRequested;

  protected AbstractTwillProgramController(ProgramId programId, TwillController twillController, RunId runId) {
    super(programId, runId);
    this.twillController = twillController;
  }

  /**
   * Get the RunId associated with the Twill controller.
   * @return the Twill RunId
   */
  public RunId getTwillRunId() {
    return twillController.getRunId();
  }

  /**
   * Starts listening to TwillController state changes. For internal use only.
   * The listener cannot be binded in constructor to avoid reference leak.
   *
   * @return this instance.
   */
  public ProgramController startListen() {
    twillController.onRunning(new Runnable() {
      @Override
      public void run() {
        LOG.info("Twill program running: {}, twill runId: {}", getProgramRunId(), twillController.getRunId());
        started();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    twillController.onTerminated(new Runnable() {
      @Override
      public void run() {
        LOG.info("Twill program terminated: {}, twill runId: {}", getProgramRunId(), twillController.getRunId());
        if (stopRequested) {
          // Service was killed
          stop();
          return;
        }

        ServiceController.TerminationStatus terminationStatus = twillController.getTerminationStatus();
        // The terminationStatus shouldn't be null when the twillController state is terminated
        // In case it does (e.g. bug or twill changes), rely on the termination future to determine the state
        if (terminationStatus == null) {
          try {
            Futures.getUnchecked(twillController.terminate());
            complete();
          } catch (Exception e) {
            complete(State.ERROR);
          }
          return;
        }
        // Based on the terminationStatus to set the completion state of the program controller.
        switch (terminationStatus) {
          case SUCCEEDED:
            complete();
            break;
          case FAILED:
            complete(State.ERROR);
            break;
          case KILLED:
            complete(State.KILLED);
            break;
          default:
            // This is just to protect against if more status are added in Twill in future
            complete();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return this;
  }

  @Override
  public void updateLogLevels(Map<String, LogEntry.Level> logLevels, @Nullable String componentName) throws Exception {
    if (componentName == null) {
      twillController.updateLogLevels(logLevels).get();
    } else {
      twillController.updateLogLevels(componentName, logLevels).get();
    }
  }

  @Override
  public void resetLogLevels(Set<String> loggerNames, @Nullable String componentName) throws Exception {
    if (componentName == null) {
      twillController.resetLogLevels(loggerNames.toArray(new String[loggerNames.size()])).get();
    } else {
      twillController.resetRunnableLogLevels(componentName, loggerNames.toArray(new String[loggerNames.size()])).get();
    }
  }

  @Override
  protected final void doSuspend() throws Exception {
    twillController.sendCommand(ProgramCommands.SUSPEND).get();
  }

  @Override
  protected final void doResume() throws Exception {
    twillController.sendCommand(ProgramCommands.RESUME).get();
  }

  @Override
  protected final void doStop() throws Exception {
    stopRequested = true;
    Futures.getUnchecked(twillController.terminate());
  }

  protected final TwillController getTwillController() {
    return twillController;
  }
}
