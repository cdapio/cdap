/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import java.util.List;
import java.util.Set;

/**
 * Request for AppConfigUpdateTask.
 */
public class AppConfigUpdateRequest {

  private final ApplicationId appId;
  private final ArtifactId artifactId;
  private final ApplicationClass applicationClass;
  private final ApplicationSpecification appSpec;
  private final List<ApplicationConfigUpdateAction> updateActions;
  private final Set<ArtifactScope> allowedArtifactScopes;
  private final boolean allowSnapshot;

  /**
   * Creates an {@link AppConfigUpdateRequest}.
   */
  public AppConfigUpdateRequest(ApplicationId appId,
                                ArtifactId artifactId,
                                ApplicationClass applicationClass,
                                ApplicationSpecification appSpec,
                                List<ApplicationConfigUpdateAction> updateActions,
                                Set<ArtifactScope> allowedArtifactScopes,
                                boolean allowSnapshot) {
    this.appId = appId;
    this.artifactId = artifactId;
    this.applicationClass = applicationClass;
    this.appSpec = appSpec;
    this.updateActions = updateActions;
    this.allowedArtifactScopes = allowedArtifactScopes;
    this.allowSnapshot = allowSnapshot;
  }

  public ApplicationId getAppId() {
    return appId;
  }

  public ArtifactId getArtifactId() {
    return artifactId;
  }

  public ApplicationClass getApplicationClass() {
    return applicationClass;
  }

  public ApplicationSpecification getAppSpec() {
    return appSpec;
  }

  public List<ApplicationConfigUpdateAction> getUpdateActions() {
    return updateActions;
  }

  public Set<ArtifactScope> getAllowedArtifactScopes() {
    return allowedArtifactScopes;
  }

  public boolean isAllowSnapshot() {
    return allowSnapshot;
  }
}
