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

package io.cdap.cdap.test.remote;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.test.AbstractProgramManager;
import io.cdap.cdap.test.WorkerManager;

/**
 * Remote implementation of {@link WorkerManager}
 */
public class RemoteWorkerManager extends AbstractProgramManager<WorkerManager> implements WorkerManager {

  private final ProgramClient programClient;
  private final ProgramId workerId;

  public RemoteWorkerManager(ProgramId programId, ClientConfig clientConfig, RESTClient restClient,
                             RemoteApplicationManager applicationManager) {
    super(programId, applicationManager);
    this.workerId = programId;
    this.programClient = new ProgramClient(clientConfig, restClient);
  }

  @Override
  public void setInstances(int instances) {
    Preconditions.checkArgument(instances > 0, "Instance count should be > 0.");
    try {
      programClient.setWorkerInstances(workerId, instances);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getInstances() {
    try {
      return programClient.getWorkerInstances(workerId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
