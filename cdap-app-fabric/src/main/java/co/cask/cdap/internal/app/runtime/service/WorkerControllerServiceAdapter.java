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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.api.service.ServiceWorker;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.ServiceWorkerDriver;
import org.apache.twill.api.RunId;

/**
 * A {@link ProgramController} for {@link ServiceWorker}s.
 */
public class WorkerControllerServiceAdapter extends ProgramControllerServiceAdapter {
  private final ServiceWorkerDriver workerDriver;

  public WorkerControllerServiceAdapter(ServiceWorkerDriver workerDriver, String programName, RunId runId) {
    super(workerDriver, programName, runId);
    this.workerDriver = workerDriver;
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    super.doCommand(name, value);
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
      return;
    }
    workerDriver.setInstanceCount((Integer) value);
  }
}
