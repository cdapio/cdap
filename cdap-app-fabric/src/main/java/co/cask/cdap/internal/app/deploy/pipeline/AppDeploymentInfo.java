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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 * Information required by application deployment pipeline {@link LocalApplicationManager}.
 */
public class AppDeploymentInfo {
  private final Id.Artifact artifactId;
  private final String appClassName;
  private final Location artifactLocation;
  private final String configString;

  public AppDeploymentInfo(Id.Artifact artifactId, String appClassName,
                           Location artifactLocation, @Nullable String configString) {
    this.artifactId = artifactId;
    this.appClassName = appClassName;
    this.artifactLocation = artifactLocation;
    this.configString = configString;
  }

  public Id.Artifact getArtifactId() {
    return artifactId;
  }

  public String getAppClassName() {
    return appClassName;
  }

  public Location getArtifactLocation() {
    return artifactLocation;
  }

  @Nullable
  public String getConfigString() {
    return configString;
  }
}
