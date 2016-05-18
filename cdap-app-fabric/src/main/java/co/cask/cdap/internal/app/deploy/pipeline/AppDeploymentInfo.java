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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 * Information required by application deployment pipeline {@link LocalApplicationManager}.
 */
public class AppDeploymentInfo {

  private final ArtifactId artifactId;
  private final Location artifactLocation;
  private final NamespaceId namespaceId;
  private final String appClassName;
  private final String appName;
  private final String configString;

  public AppDeploymentInfo(ArtifactDescriptor artifactDescriptor, NamespaceId namespaceId,
                           String appClassName, @Nullable String appName, @Nullable String configString) {
    this.artifactId = Artifacts.toArtifactId(namespaceId, artifactDescriptor.getArtifactId());
    this.artifactLocation = artifactDescriptor.getLocation();
    this.namespaceId = namespaceId;
    this.appClassName = appClassName;
    this.appName = appName;
    this.configString = configString;
  }

  /**
   * Returns the {@link ArtifactId} used by the application.
   */
  public ArtifactId getArtifactId() {
    return artifactId;
  }

  /**
   * Returns the {@link Location} to the artifact that is used by the application.
   */
  public Location getArtifactLocation() {
    return artifactLocation;
  }

  /**
   * Returns the {@link NamespaceId} that the application is deploying to.
   */
  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  /**
   * Returns the class name of the {@link Application}.
   */
  public String getAppClassName() {
    return appClassName;
  }

  /**
   * Returns the name of the application or {@code null} if is it not provided.
   */
  @Nullable
  public String getApplicationName() {
    return appName;
  }

  /**
   * Returns the configuration string provided for the application deployment or {@code null} if it is not provided.
   */
  @Nullable
  public String getConfigString() {
    return configString;
  }
}
