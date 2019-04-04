/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;

import java.util.Set;

/**
 * {@link EntityExistenceVerifier} for {@link ProgramId programs}.
 */
public class ProgramExistenceVerifier implements EntityExistenceVerifier<ProgramId> {
  private final Store store;

  @Inject
  ProgramExistenceVerifier(Store store) {
    this.store = store;
  }

  @Override
  public void ensureExists(ProgramId programId) throws ApplicationNotFoundException, ProgramNotFoundException {
    ApplicationId appId = programId.getParent();
    ApplicationSpecification appSpec = store.getApplication(appId);
    if (appSpec == null) {
      throw new ApplicationNotFoundException(appId);
    }
    ProgramType programType = programId.getType();

    Set<String> programNames = null;
    if (programType == ProgramType.MAPREDUCE && appSpec.getMapReduce() != null) {
      programNames = appSpec.getMapReduce().keySet();
    } else if (programType == ProgramType.WORKFLOW && appSpec.getWorkflows() != null) {
      programNames = appSpec.getWorkflows().keySet();
    } else if (programType == ProgramType.SERVICE && appSpec.getServices() != null) {
      programNames = appSpec.getServices().keySet();
    } else if (programType == ProgramType.SPARK && appSpec.getSpark() != null) {
      programNames = appSpec.getSpark().keySet();
    } else if (programType == ProgramType.WORKER && appSpec.getWorkers() != null) {
      programNames = appSpec.getWorkers().keySet();
    }

    if (programNames != null) {
      if (programNames.contains(programId.getProgram())) {
        // is valid.
        return;
      }
    }
    throw new ProgramNotFoundException(programId);
  }
}
