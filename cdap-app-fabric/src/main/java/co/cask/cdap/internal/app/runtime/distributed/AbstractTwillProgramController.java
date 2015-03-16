/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.TwillController;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} that control program through twill.
 */
abstract class AbstractTwillProgramController extends AbstractProgramController implements ProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillProgramController.class);

  protected final String programName;
  protected final TwillController twillController;

  protected AbstractTwillProgramController(String programName, TwillController twillController) {
    super(programName, twillController.getRunId());
    this.programName = programName;
    this.twillController = twillController;
  }

  /**
   * Starts listening to TwillController state changes. For internal use only.
   * The listener cannot be binded in constructor to avoid reference leak.
   *
   * @return this instance.
   */
  public ProgramController startListen() {
    twillController.addListener(createTwillListener(), Threads.SAME_THREAD_EXECUTOR);
    return this;
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
    twillController.stopAndWait();
  }

  private TwillController.Listener createTwillListener() {
    return new ServiceListenerAdapter() {

      @Override
      public void running() {
        LOG.info("Twill program running: {} {}", programName, twillController.getRunId());
        started();
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Twill program terminated: {} {}", programName, twillController.getRunId());
        if (getState() != State.STOPPING) {
          // Service completed by itself. Simply signal the state change of this controller.
          complete();
        } else {
          // Service was killed
          stop();
        }
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Twill program failed: {} {}", programName, twillController.getRunId());
        error(failure);
      }
    };
  }
}
