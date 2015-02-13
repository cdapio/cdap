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

import co.cask.cdap.app.program.Program;
import org.apache.twill.api.TwillController;

/**
 * For updating the number of worker instances.
 */
public class DistributedWorkerInstanceUpdater {
  private final Program program;
  private final TwillController twillController;

  DistributedWorkerInstanceUpdater(Program program, TwillController twillController) {
    this.program = program;
    this.twillController = twillController;
  }

  void update(String runnableId, int newInstanceCount, int oldInstanceCount) throws Exception {
    twillController.changeInstances(runnableId, newInstanceCount).get();
  }
}
