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
import io.cdap.cdap.internal.operations.OperationRunDispatcher;
import io.cdap.cdap.proto.ProgramType;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgramRunDispatcherFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunDispatcherFactory.class);

  private final InMemoryProgramRunDispatcher inMemoryProgramRunDispatcher;
  private final boolean workerPoolEnabled;
  private final Set<ProgramType> remoteDispatchProgramTypes;
  private ProgramRunDispatcher remoteProgramRunDispatcher;

  //TODO have one remote and one inmemory operation dispatcher
  private final OperationRunDispatcher operationRunDispatcher;

  @Inject
  public ProgramRunDispatcherFactory(CConfiguration cConf,
      InMemoryProgramRunDispatcher inMemoryProgramRunDispatcher,
      OperationRunDispatcher operationRunDispatcher) {
    this.inMemoryProgramRunDispatcher = inMemoryProgramRunDispatcher;
    this.operationRunDispatcher = operationRunDispatcher;
    this.workerPoolEnabled = cConf.getBoolean(Constants.SystemWorker.POOL_ENABLE);
    this.remoteDispatchProgramTypes = new HashSet<>();
    String[] programTypes = cConf.getStrings(Constants.SystemWorker.DISPATCH_PROGRAM_TYPES);
    if (programTypes != null) {
      for (String type : programTypes) {
        try {
          remoteDispatchProgramTypes.add(ProgramType.valueOf(type.toUpperCase()));
        } catch (IllegalArgumentException e) {
          LOG.warn("Skipping invalid program type {} for remote dispatch due to {}", type, e);
        }
      }
    }
  }

  /**
   * For unit tests, RemoteProgramRunDispatcher would not be set.
   */
  @Inject(optional = true)
  public void setRemoteProgramRunDispatcher(ProgramRunDispatcher programRunDispatcher) {
    this.remoteProgramRunDispatcher = programRunDispatcher;
  }

  public ProgramRunDispatcher getProgramRunDispatcher(ProgramType type) {
    if (type == ProgramType.Operation) {
      return operationRunDispatcher;
    }
    // Returns InMemoryProgramRunDispatcher if remoteDispatcher is not set.
    return workerPoolEnabled && remoteDispatchProgramTypes.contains(type)
        && remoteProgramRunDispatcher != null
        ? remoteProgramRunDispatcher : inMemoryProgramRunDispatcher;
  }
}
