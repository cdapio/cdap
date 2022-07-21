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
package io.cdap.cdap.internal.app.runtime.distributed;

import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public final class WorkflowTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTwillProgramController.class);

  public WorkflowTwillProgramController(ProgramRunId programRunId, TwillController controller) {
    super(programRunId, controller);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // Workflow doesn't have any command for now.
    LOG.info("Command ignored for workflow controller: {}, {}", name, value);
  }
}
