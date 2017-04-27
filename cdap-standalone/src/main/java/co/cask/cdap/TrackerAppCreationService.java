/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Create Tracker App in Default Namespace and start its Programs during Standalone startup.
 */
public class TrackerAppCreationService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(TrackerAppCreationService.class);
  private static final String TRACKER_CONFIG = "tracker.app.config";
  private static final ApplicationId TRACKER_APPID = NamespaceId.DEFAULT.app("_Tracker");
  private static final ProgramId AUDIT_FLOWID = TRACKER_APPID.flow("AuditLogFlow");
  private static final ProgramId TRACKER_SERVICEID = TRACKER_APPID.service("TrackerService");

  private final CConfiguration cConf;
  private final ArtifactRepository artifactRepository;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final ProgramLifecycleService programLifecycleService;
  private volatile boolean stopping = false;

  @Inject
  public TrackerAppCreationService(CConfiguration cConf, ArtifactRepository artifactRepository,
                                   ApplicationLifecycleService applicationLifecycleService,
                                   ProgramLifecycleService programLifecycleService) {
    this.cConf = cConf;
    this.artifactRepository = artifactRepository;
    this.applicationLifecycleService = applicationLifecycleService;
    this.programLifecycleService = programLifecycleService;
  }

  @Override
  protected void run() throws Exception {
    try {
      // Wait until tracker artifact is available
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          // Exit if stop has been triggered
          if (stopping) {
            return true;
          }

          List<ArtifactSummary> artifacts = null;
          try {
            artifacts = artifactRepository.getArtifactSummaries(NamespaceId.SYSTEM, "tracker", -1, null);
          } catch (ArtifactNotFoundException ex) {
            // expected, so suppress it
          }
          return artifacts != null && !artifacts.isEmpty();
        }
      }, 5, TimeUnit.MINUTES, 2, TimeUnit.SECONDS, "Waiting for Tracker Artifact to become available.");
      // Find the latest tracker artifact based on the version
      List<ArtifactSummary> artifacts = new ArrayList<>(artifactRepository.getArtifactSummaries(NamespaceId.SYSTEM,
                                                                                                "tracker", -1, null));
      if (stopping) {
        LOG.debug("TrackerAppService shutting down.");
        // Service was stopped, otherwise a TimeoutException would have been thrown
        return;
      }

      ArtifactSummary artifactSummary = artifacts.remove(0);
      for (ArtifactSummary artifact : artifacts) {
        ArtifactVersion old = new ArtifactVersion(artifactSummary.getVersion());
        ArtifactVersion current = new ArtifactVersion(artifact.getVersion());
        if (current.compareTo(old) > 0) {
          artifactSummary = artifact;
        }
      }
      createAndStartTracker(artifactSummary);
    } catch (Exception ex) {
      // Not able to create Tracker App is not catastrophic, hence don't propagate exception
      LOG.warn("Got an exception while trying to create and start Tracker App. ", ex);
    }
  }

  @Override
  protected void triggerShutdown() {
    stopping = true;
    super.triggerShutdown();
  }

  private void createAndStartTracker(ArtifactSummary artifactSummary) throws Exception {
    String trackerAppConfig = cConf.get(TRACKER_CONFIG);
    LOG.info("Creating and starting Tracker App with config : {}", trackerAppConfig);
    applicationLifecycleService.deployApp(
      TRACKER_APPID.getParent(), TRACKER_APPID.getApplication(), TRACKER_APPID.getVersion(),
      NamespaceId.SYSTEM.artifact(artifactSummary.getName(), artifactSummary.getVersion()).toId(),
      trackerAppConfig, new TrackerProgramTerminator(programLifecycleService));
    try {
      programLifecycleService.start(AUDIT_FLOWID, ImmutableMap.<String, String>of(), false);
    } catch (IOException ex) {
      // Might happen if the program is being started in parallel through UI
      LOG.debug("Error while trying to start Tracker's AuditFlow. {}", ex.getMessage());
    }
    try {
      programLifecycleService.start(TRACKER_SERVICEID, ImmutableMap.<String, String>of(), false);
    } catch (IOException ex) {
      // Might happen if the program is being started in parallel through UI
      LOG.debug("Error while trying to start Tracker's AuditService. {}", ex.getMessage());
    }
  }

  private class TrackerProgramTerminator implements ProgramTerminator {
    private final ProgramLifecycleService programLifecycleService;

    TrackerProgramTerminator(ProgramLifecycleService programLifecycleService) {
      this.programLifecycleService = programLifecycleService;
    }

    @Override
    public void stop(ProgramId programId) throws Exception {
      switch (programId.getType()) {
        case FLOW:
          programLifecycleService.stop(AUDIT_FLOWID);
          break;
        case SERVICE:
          programLifecycleService.stop(TRACKER_SERVICEID);
          break;
        default:
          LOG.warn("Found an unexpected program {} in TrackerApp.", programId);
      }
    }
  }
}
