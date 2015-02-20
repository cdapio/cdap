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

import co.cask.cdap.internal.app.worker.WorkerProgramRunner;

/**
 * A TwillRunnable for running Workers in distributed mode.
 */
public class WorkerTwillRunnable extends AbstractProgramTwillRunnable<WorkerProgramRunner> {

  protected WorkerTwillRunnable(String name, String hConfName, String cConfName) {
    super(name, hConfName, cConfName);
  }

  @Override
  protected Class<WorkerProgramRunner> getProgramClass() {
    return WorkerProgramRunner.class;
  }
}
