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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A ProgramController for Workers that are launched through Twill.
 */
public class WorkerTwillProgramController extends AbstractTwillProgramController {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerTwillProgramController.class);
  private static final int MAX_WAIT_SECONDS = 30;
  private static final int SECONDS_PER_WAIT = 1;

  private final TwillController controller;

  WorkerTwillProgramController(String programId, TwillController controller) {
    super(programId, controller);
    this.controller = controller;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, String> command = (Map<String, String>) value;
    try {
      for (Map.Entry<String, String> entry : command.entrySet()) {
        LOG.info("Changing worker instance count: {} new count is: {}", entry.getKey(), entry.getValue());
        changeInstances(entry.getKey(), getNumberOfProvisionedInstances(entry.getKey()),
                        Integer.valueOf(entry.getValue()));
        LOG.info("Worker instance count changed: {} new count is {}", entry.getKey(), entry.getValue());
      }
    } catch (Throwable t) {
      LOG.error("Failed to change worker instances : {}", command, t);
    }
  }

  private synchronized void changeInstances(String workerId, int oldInstanceCount, int newInstanceCount)
    throws Exception {
    waitForInstances(workerId, oldInstanceCount);
    twillController.sendCommand(workerId, ProgramCommands.SUSPEND).get();
    controller.changeInstances(workerId, newInstanceCount).get();
    twillController.sendCommand(workerId, ProgramCommands.RESUME).get();
  }

  private void waitForInstances(String workerId, int expectedInstances) throws InterruptedException, TimeoutException {
    int numRunningFlowlets = getNumberOfProvisionedInstances(workerId);
    int secondsWaited = 0;
    while (numRunningFlowlets != expectedInstances) {
      LOG.debug("waiting for {} instances of {} before suspending flowlets", expectedInstances, workerId);
      TimeUnit.SECONDS.sleep(SECONDS_PER_WAIT);
      secondsWaited += SECONDS_PER_WAIT;
      if (secondsWaited > MAX_WAIT_SECONDS) {
        String errmsg =
          String.format("waited %d seconds for instances of %s to reach expected count of %d, but %d are running",
                        secondsWaited, workerId, expectedInstances, numRunningFlowlets);
        LOG.error(errmsg);
        throw new TimeoutException(errmsg);
      }
      numRunningFlowlets = getNumberOfProvisionedInstances(workerId);
    }
  }

  private int getNumberOfProvisionedInstances(String flowletId) {
    return twillController.getResourceReport().getRunnableResources(flowletId).size();
  }
}
