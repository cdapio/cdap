/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.exception.WrappedException;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.List;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 */
final class WorkflowProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramController.class);

  private final WorkflowDriver driver;
  private final String serviceName;

  WorkflowProgramController(ProgramRunId programRunId, WorkflowDriver driver) {
    super(programRunId);
    this.driver = driver;
    this.serviceName = getServiceName();
    startListen(driver);
  }

  @Override
  protected void doSuspend() throws Exception {
    driver.suspend();
  }

  @Override
  protected void doResume() throws Exception {
    driver.resume();
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    LOG.info("Command ignored {}, {}", name, value);
  }

  private void startListen(Service service) {
    // Forward state changes from the given service to this controller.
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {

        LOG.debug("Workflow service {} started", serviceName);
        started();
      }

      @Override
      public void terminated(Service.State from) {
        LOG.debug("Workflow service terminated from {}. Un-registering service {}.", from,
            serviceName);
        if (getState() != State.STOPPING) {
          // service completed itself.
          complete();
        } else {
          // service was terminated
          stop();
        }
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        List<Throwable> causalChain = Throwables.getCausalChain(failure);
        for(Throwable cause : causalChain) {
          if (cause instanceof WrappedException) {
            String stageName = ((WrappedException) cause).getStageName();
            LOG.error("Stage: {}", stageName);
            MDC.put("Failed_Stage", stageName);
            break;
          }
        }
        LOG.error("Workflow service '{}' failed.", serviceName, failure);
        error(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private String getServiceName() {
    ProgramId programId = getProgramRunId().getParent();
    RunId runId = getRunId();
    return String.format("workflow.%s.%s.%s.%s",
        programId.getNamespace(), programId.getApplication(), programId.getProgram(),
        runId.getId());
  }
}
