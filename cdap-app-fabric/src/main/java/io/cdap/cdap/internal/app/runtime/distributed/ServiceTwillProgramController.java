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

package io.cdap.cdap.internal.app.runtime.distributed;

import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *  A ProgramController for Services that are launched through Twill.
 */
final class ServiceTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceTwillProgramController.class);

  ServiceTwillProgramController(ProgramRunId programRunId, TwillController controller) {
    super(programRunId, controller);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doCommand(String name, Object value) throws Exception {
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
      return;
    }

    Map<String, String> command = (Map<String, String>) value;
    try {
      changeInstances(command.get("runnable"),
                      Integer.parseInt(command.get("newInstances")),
                      Integer.parseInt(command.get("oldInstances")));
    } catch (Throwable t) {
      LOG.error(String.format("Failed to change instances: %s", command), t);
      throw t;
    }
  }

  private synchronized void changeInstances(String runnableId,
                                            int newInstanceCount, int oldInstanceCount) throws Exception {
    LOG.info("Changing instances of {} from {} to {}", getProgramRunId(), oldInstanceCount, newInstanceCount);
    getTwillController().changeInstances(runnableId, newInstanceCount).get();
    LOG.info("Completed changing instances of {} from {} to {}", getProgramRunId(), oldInstanceCount, newInstanceCount);

  }
}
