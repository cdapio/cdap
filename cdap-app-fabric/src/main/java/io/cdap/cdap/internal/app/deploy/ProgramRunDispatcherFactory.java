/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy;

import com.google.inject.Inject;
import io.cdap.cdap.app.deploy.ProgramRunDispatcher;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.ProgramType;

import java.util.Objects;

public class ProgramRunDispatcherFactory {

  private final CConfiguration cConf;
  private final InMemoryProgramRunDispatcher inMemoryProgramRunDispatcher;
  private RemoteProgramRunDispatcher remoteProgramRunDispatcher;

  @Inject
  public ProgramRunDispatcherFactory(CConfiguration cConf, InMemoryProgramRunDispatcher inMemoryProgramRunDispatcher) {
    this.cConf = cConf;
    this.inMemoryProgramRunDispatcher = inMemoryProgramRunDispatcher;
  }

  /**
   * For unit tests, RemoteProgramRunDispatcher would not be set.
   */
  @Inject(optional = true)
  public void setRemoteProgramRunDispatcher(RemoteProgramRunDispatcher remoteProgramRunDispatcher) {
    this.remoteProgramRunDispatcher = remoteProgramRunDispatcher;
  }

  public ProgramRunDispatcher getProgramRunDispatcher(ProgramType type) {
    // TODO(CDAP-18962): Get ProgramTypes that can be dispatched remotely from configuration.
    boolean workerPoolEnabled = cConf.getBoolean(Constants.SystemWorker.POOL_ENABLE);
    // Returns InMemoryProgramRunDispatcher if remoteDispatcher is not set.
    return workerPoolEnabled && type.equals(ProgramType.WORKFLOW) && Objects.nonNull(remoteProgramRunDispatcher)
      ? remoteProgramRunDispatcher : inMemoryProgramRunDispatcher;
  }
}
