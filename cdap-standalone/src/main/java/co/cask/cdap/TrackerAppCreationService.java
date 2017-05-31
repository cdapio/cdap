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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.ApplicationLifecycleService;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Map;

/**
 * Create Tracker App in Default Namespace and start its Programs during Standalone startup.
 */
public class TrackerAppCreationService extends AbstractAppCreationService {

  private static final String TRACKER_CONFIG = "tracker.app.config";
  private static final ApplicationId TRACKER_APPID = NamespaceId.DEFAULT.app("_Tracker");
  private static final ProgramId AUDIT_FLOWID = TRACKER_APPID.flow("AuditLogFlow");
  private static final ProgramId TRACKER_SERVICEID = TRACKER_APPID.service("TrackerService");

  private static final Map<ProgramId, Map<String, String>> PROGRAM_ID_MAP =
    ImmutableMap.<ProgramId, Map<String, String>>of(AUDIT_FLOWID, ImmutableMap.<String, String>of(),
                                                    TRACKER_SERVICEID, ImmutableMap.<String, String>of());

  @Inject
  public TrackerAppCreationService(CConfiguration cConf, ArtifactRepository artifactRepository,
                                   ApplicationLifecycleService applicationLifecycleService,
                                   ProgramLifecycleService programLifecycleService) {
    super(artifactRepository, applicationLifecycleService, programLifecycleService, "tracker", TRACKER_APPID,
          PROGRAM_ID_MAP, cConf.get(TRACKER_CONFIG, ""));
  }
}
