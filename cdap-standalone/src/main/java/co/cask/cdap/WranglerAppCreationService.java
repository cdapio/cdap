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
 * Create Wrangler App in Default Namespace and start its program during Standalone startup.
 */
public class WranglerAppCreationService extends AbstractAppCreationService {

  private static final String WRANGLER_CONIFG = "wrangler.app.config";
  private static final ApplicationId WRANGLER_APPID = NamespaceId.DEFAULT.app("dataprep");
  private static final ProgramId WRANGLER_SERVICEID = WRANGLER_APPID.service("service");

  private static final Map<ProgramId, Map<String, String>> PROGRAM_ID_MAP =
    ImmutableMap.<ProgramId, Map<String, String>>of(WRANGLER_SERVICEID, ImmutableMap.<String, String>of());


  @Inject
  public WranglerAppCreationService(CConfiguration cConf, ArtifactRepository artifactRepository,
                                    ApplicationLifecycleService applicationLifecycleService,
                                    ProgramLifecycleService programLifecycleService) {
    super(artifactRepository, applicationLifecycleService, programLifecycleService,
          "wrangler-service", WRANGLER_APPID, PROGRAM_ID_MAP, cConf.get(WRANGLER_CONIFG, ""));
  }
}
