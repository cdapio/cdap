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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.ProgramController;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.RunId;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} implementation that control a guava Service.
 */
public class ProgramControllerServiceAdapter extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramControllerServiceAdapter.class);

  private final Service service;

  public ProgramControllerServiceAdapter(Service service, String programName, RunId runId) {
    super(programName, runId);
    this.service = service;
    listenToRuntimeState(service);
  }

  @Override
  protected void doSuspend() throws Exception {

  }

  @Override
  protected void doResume() throws Exception {

  }

  @Override
  protected void doStop() throws Exception {
    if (service.state() != Service.State.TERMINATED && service.state() != Service.State.FAILED) {
      service.stopAndWait();
    }
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {

  }

  /**
   * Returns {@code true} if service error is propagated as error for this controller state.
   * If this method returns {@code false}, service error is just getting logged and state will just change
   * to {@link State#STOPPED}. This method returns {@code true} by default.
   */
  protected boolean propagateServiceError() {
    return true;
  }

  private void listenToRuntimeState(Service service) {
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        started();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.error("Program terminated with exception", failure);
        if (propagateServiceError()) {
          error(failure);
        } else if (getState() != State.STOPPING) {
          stop();
        }
      }

      @Override
      public void terminated(Service.State from) {
        if (getState() != State.STOPPING) {
          // Service completed by itself. Simply signal the state change of this controller.
          stop();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}
