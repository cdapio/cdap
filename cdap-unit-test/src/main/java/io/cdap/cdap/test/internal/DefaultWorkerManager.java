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

package co.cask.cdap.test.internal;

import co.cask.cdap.common.id.Id;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.test.AbstractProgramManager;
import co.cask.cdap.test.WorkerManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A default implementation of {@link WorkerManager}
 */
public class DefaultWorkerManager extends AbstractProgramManager<WorkerManager> implements WorkerManager {
  private final AppFabricClient appFabricClient;

  public DefaultWorkerManager(Id.Program programId,
                              AppFabricClient appFabricClient, DefaultApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.appFabricClient = appFabricClient;
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instance count should be > 0.");
    try {
      appFabricClient.setWorkerInstances(programId.getNamespace(), programId.getApplication(), programId.getProgram(),
                                         instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getInstances() {
    try {
      return appFabricClient.getWorkerInstances(programId.getNamespace(), programId.getApplication(),
                                                programId.getProgram()).getInstances();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
