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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.proto.Id;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProgramController} for {@link Spark} program in distributed mode.
 */
final class SparkTwillProgramController extends AbstractTwillProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTwillProgramController.class);

  SparkTwillProgramController(Id.Program programId, TwillController controller, RunId runId) {
    super(programId, controller, runId);
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    // Spark doesn't have any command for now.
    LOG.info("Command ignored for spark controller: {}, {}", name, value);
  }
}
