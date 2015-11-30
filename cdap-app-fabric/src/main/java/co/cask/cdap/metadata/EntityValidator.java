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

package co.cask.cdap.metadata;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.ViewNotFoundException;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Set;

/**
 * Helper class for entity validation.
 */
public class EntityValidator {
  private final AbstractNamespaceClient namespaceClient;
  private final Store store;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;
  private final ArtifactStore artifactStore;

  @Inject
  EntityValidator(AbstractNamespaceClient namespaceClient, Store store, DatasetFramework datasetFramework,
                  StreamAdmin streamAdmin, ArtifactStore artifactStore) {
    this.namespaceClient = namespaceClient;
    this.store = store;
    this.datasetFramework = datasetFramework;
    this.streamAdmin = streamAdmin;
    this.artifactStore = artifactStore;
  }

  /**
   * Ensures that the specified {@link Id.NamespacedId} exists.
   */
  public void ensureEntityExists(Id.NamespacedId entityId) throws NotFoundException {
    try {
      namespaceClient.get(entityId.getNamespace());
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    // Check existence of entity
    if (entityId instanceof Id.Program) {
      Id.Program program = (Id.Program) entityId;
      Id.Application application = program.getApplication();
      ApplicationSpecification appSpec = store.getApplication(application);
      if (appSpec == null) {
        throw new ApplicationNotFoundException(application);
      }
      ensureProgramExists(appSpec, program);
    } else if (entityId instanceof Id.Application) {
      Id.Application application = (Id.Application) entityId;
      if (store.getApplication(application) == null) {
        throw new ApplicationNotFoundException(application);
      }
    } else if (entityId instanceof Id.DatasetInstance) {
      Id.DatasetInstance datasetInstance = (Id.DatasetInstance) entityId;
      try {
        if (!datasetFramework.hasInstance(datasetInstance)) {
          throw new DatasetNotFoundException(datasetInstance);
        }
      } catch (DatasetManagementException ex) {
        throw new IllegalStateException(ex);
      }
    } else if (entityId instanceof Id.Stream) {
      Id.Stream stream = (Id.Stream) entityId;
      try {
        if (!streamAdmin.exists(stream)) {
          throw new StreamNotFoundException(stream);
        }
      } catch (StreamNotFoundException streamEx) {
        throw streamEx;
      } catch (Exception ex) {
        throw new IllegalStateException(ex);
      }
    } else if (entityId instanceof Id.Artifact) {
      Id.Artifact artifactId = (Id.Artifact) entityId;
      try {
        artifactStore.getArtifact(artifactId);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (entityId instanceof Id.Stream.View) {
      Id.Stream.View viewId = (Id.Stream.View) entityId;
      try {
        if (!streamAdmin.viewExists(viewId)) {
          throw new ViewNotFoundException(viewId);
        }
      } catch (ViewNotFoundException | StreamNotFoundException viewEx) {
        throw viewEx;
      } catch (Exception ex) {
        throw new IllegalStateException(ex);
      }
    } else {
      throw new IllegalArgumentException("Invalid entity" + entityId);
    }
  }

  /**
   * Ensures that the specified run exists.
   */
  public void ensureRunExists(Id.Run run) throws NotFoundException {
    ensureEntityExists(run.getProgram());
    if (store.getRun(run.getProgram(), run.getId()) == null) {
      throw new NotFoundException("Run " + run.getId() + " does not exist");
    }
  }

  private void ensureProgramExists(ApplicationSpecification appSpec, Id.Program program) throws NotFoundException {
    ProgramType programType = program.getType();

    Set<String> programNames = null;
    if (programType == ProgramType.FLOW && appSpec.getFlows() != null) {
      programNames = appSpec.getFlows().keySet();
    } else if (programType == ProgramType.MAPREDUCE && appSpec.getMapReduce() != null) {
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
      if (programNames.contains(program.getId())) {
        // is valid.
        return;
      }
    }
    throw new ProgramNotFoundException(program);
  }
}
