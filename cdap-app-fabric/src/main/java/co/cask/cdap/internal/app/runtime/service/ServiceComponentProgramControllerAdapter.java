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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.internal.app.services.ServiceWorkerDriver;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.RunId;

/**
 * A {@link ProgramControllerServiceAdapter} that updates instance counts for {@link Service} component contexts.
 */
public class ServiceComponentProgramControllerAdapter extends ProgramControllerServiceAdapter {
  private final Service service;

  public ServiceComponentProgramControllerAdapter(Service service, String programName, RunId runId) {
    super(service, programName, runId);
    this.service = service;
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    super.doCommand(name, value);
    if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Integer)) {
      return;
    }
    if (service instanceof ServiceWorkerDriver) {
      ((ServiceWorkerDriver) service).setInstanceCount((Integer) value);
    } else if (service instanceof ServiceHttpServer) {
      ((ServiceHttpServer) service).setInstanceCount((Integer) value);
    } else {
      throw new IllegalArgumentException("Can not increase instances for an unknown service type.");
    }
  }
}
