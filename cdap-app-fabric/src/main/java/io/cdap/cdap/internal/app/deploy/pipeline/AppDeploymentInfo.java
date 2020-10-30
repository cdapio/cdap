/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 * Information required by application deployment pipeline {@link LocalApplicationManager}.
 */
public class AppDeploymentInfo {

  private final ArtifactId artifactId;
  private final Location artifactLocation;
  private final NamespaceId namespaceId;
  private final ApplicationClass applicationClass;
  private final String appName;
  private final String appVersion;
  private final String configString;
  @SerializedName("principal")
  private final KerberosPrincipalId ownerPrincipal;
  @SerializedName("update-schedules")
  private final boolean updateSchedules;

  public AppDeploymentInfo(ArtifactDescriptor artifactDescriptor, NamespaceId namespaceId,
                           ApplicationClass applicationClass, @Nullable String appName, @Nullable String appVersion,
                           @Nullable String configString) {
    this(artifactDescriptor, namespaceId, applicationClass, appName, appVersion, configString, null, true);
  }

  public AppDeploymentInfo(ArtifactDescriptor artifactDescriptor, NamespaceId namespaceId,
                           ApplicationClass applicationClass, @Nullable String appName, @Nullable String appVersion,
                           @Nullable String configString, @Nullable KerberosPrincipalId ownerPrincipal,
                           boolean updateSchedules) {
    this.artifactId = Artifacts.toProtoArtifactId(namespaceId, artifactDescriptor.getArtifactId());
    this.artifactLocation = artifactDescriptor.getLocation();
    this.namespaceId = namespaceId;
    this.applicationClass = applicationClass;
    this.appName = appName;
    this.appVersion = appVersion;
    this.configString = configString;
    this.ownerPrincipal = ownerPrincipal;
    this.updateSchedules = updateSchedules;
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
   * Returns the {@link ApplicationClass} associated with this {@link Application}.
   */
  public ApplicationClass getApplicationClass() {
    return applicationClass;
  }

  /**
   * Returns the name of the application or {@code null} if is it not provided.
   */
  @Nullable
  public String getApplicationName() {
    return appName;
  }

  /**
   * Returns the version of the application or {@code null} if is it not provided.
   */
  @Nullable
  public String getApplicationVersion() {
    return appVersion;
  }

  /**
   * Returns the configuration string provided for the application deployment or {@code null} if it is not provided.
   */
  @Nullable
  public String getConfigString() {
    return configString;
  }

  /**
   * @return the principal of the application owner
   */
  @Nullable
  public KerberosPrincipalId getOwnerPrincipal() {
    return ownerPrincipal;
  }

  /**
   * return true if we can update the schedules of the app
   */
  public boolean canUpdateSchedules() {
    return updateSchedules;
  }
}
