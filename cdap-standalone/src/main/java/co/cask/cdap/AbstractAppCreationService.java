/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.artifact.ArtifactSortOrder;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Service that creates an application in default namespace and starts programs during Standalone startup.
 */
class AbstractAppCreationService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAppCreationService.class);

  private final ArtifactRepository artifactRepository;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;

  private final String artifactName;
  private final ApplicationId appId;
  private final Map<ProgramId, Map<String, String>> programIdMap;
  private final String appConfig;

  private volatile boolean stopping = false;

  AbstractAppCreationService(ArtifactRepository artifactRepository,
                             ApplicationLifecycleService applicationLifecycleService,
                             ProgramLifecycleService programLifecycleService,
                             String artifactName, ApplicationId appId,
                             Map<ProgramId, Map<String, String>> programIdMap,
                             String appConfig) {
    this.artifactRepository = artifactRepository;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
    this.artifactName = artifactName;
    this.appId = appId;
    this.programIdMap = programIdMap;
    this.appConfig = appConfig;
  }

  @Override
  protected void run() throws Exception {
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          // Exit if stop has been triggered.
          if (stopping) {
            return true;
          }

          List<ArtifactSummary> artifacts = null;
          try {
            artifacts = artifactRepository.getArtifactSummaries(NamespaceId.SYSTEM, artifactName,
                                                                Integer.MAX_VALUE, ArtifactSortOrder.DESC);
          } catch (ArtifactNotFoundException ex) {
            // expected, so suppress it
          }
          return artifacts != null && !artifacts.isEmpty();
        }
      }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS, String.format("Waiting for %s artifact to become available.",
                                                                 artifactName));

      // Get all artifacts present in system namespace
      List<ArtifactSummary> artifacts = new ArrayList<>(artifactRepository.getArtifactSummaries(
        NamespaceId.SYSTEM, artifactName, Integer.MAX_VALUE, ArtifactSortOrder.DESC));

      // Get all artifacts present in the user scope if it exists
      List<ArtifactSummary> userArtifacts = new ArrayList<>();
      try {
        userArtifacts.addAll(artifactRepository.getArtifactSummaries(
          appId.getNamespaceId(), artifactName, Integer.MAX_VALUE, ArtifactSortOrder.DESC));
      } catch (ArtifactNotFoundException ex) {
        // expected if no user artifact is present, hence suppress it.
      }

      artifacts.addAll(userArtifacts);
      ArtifactSummary maxSummary = artifacts.get(0);

      // Find the artifact with the highest artifact version
      for (ArtifactSummary currentSummary : artifacts) {
        ArtifactVersion currentVersion = new ArtifactVersion(currentSummary.getVersion());
        ArtifactVersion maxVersion = new ArtifactVersion(maxSummary.getVersion());
        if (currentVersion.compareTo(maxVersion) > 0) {
          maxSummary = currentSummary;
        }
      }

      if (stopping) {
        LOG.debug("{} AppCreationService is shutting down.", appId.getApplication());
      }
      createAppAndStartProgram(maxSummary);
    } catch (Exception ex) {
      // Not able to create application. But it is not catastrophic, hence just log a warning.
      LOG.warn("Got an exception while trying to create and start {} app.", appId, ex);
    }
  }

  @Override
  protected void triggerShutdown() {
    stopping = true;
    super.triggerShutdown();
  }

  private void createAppAndStartProgram(ArtifactSummary artifactSummary) throws Exception {
    LOG.info("Creating and Starting {} App with config : {}", appId.getApplication(), appConfig);

    ArtifactId artifactId = artifactSummary.getScope().equals(ArtifactScope.SYSTEM) ?
      NamespaceId.SYSTEM.artifact(artifactSummary.getName(), artifactSummary.getVersion()) :
      appId.getNamespaceId().artifact(artifactSummary.getName(), artifactSummary.getVersion());

    applicationLifecycleService.deployApp(appId.getParent(), appId.getApplication(), appId.getVersion(),
                                          Id.Artifact.fromEntityId(artifactId),
                                          appConfig, new DefaultProgramTerminator());

    for (Map.Entry<ProgramId, Map<String, String>> programEntry : programIdMap.entrySet()) {
      try {
        programLifecycleService.run(programEntry.getKey(), programEntry.getValue(), false);
      } catch (IOException ex) {
        // Might happen if the program is being started in parallel through UI
        LOG.debug("Tried to start {} program but had a conflict. {}", programEntry.getKey(), ex.getMessage());
      }
    }
  }

  private class DefaultProgramTerminator implements ProgramTerminator {

    @Override
    public void stop(ProgramId programId) throws Exception {
      if (programIdMap.containsKey(programId)) {
        programLifecycleService.stop(programId);
      } else {
        LOG.warn("Found an unexpected program {} in {}", programId, appId);
      }
    }
  }
}
