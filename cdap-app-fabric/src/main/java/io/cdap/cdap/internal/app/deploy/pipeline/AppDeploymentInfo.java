/*
 * Copyright © 2015-2022 Cask Data, Inc.
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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.internal.app.deploy.LocalApplicationManager;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;

/**
 * Information required by application deployment pipeline {@link LocalApplicationManager}.
 */
public class AppDeploymentInfo {

  private final ArtifactId artifactId;
  private final transient Location artifactLocation;
  private final NamespaceId namespaceId;
  private final ApplicationClass applicationClass;
  @Nullable
  private final String appName;
  @Nullable
  private final String appVersion;
  @Nullable
  private final String configString;
  @Nullable
  @SerializedName("principal")
  private final KerberosPrincipalId ownerPrincipal;
  @SerializedName("update-schedules")
  private final boolean updateSchedules;
  @Nullable
  private final AppDeploymentRuntimeInfo runtimeInfo;
  @Nullable
  private final ChangeDetail changeDetail;
  @Nullable
  private final SourceControlMeta sourceControlMeta;
  @Nullable
  private final ApplicationSpecification deployedApplicationSpec;
  private final boolean isUpgrade;
  private final boolean skipMarkingLatest;

  /**
   * Creates a new {@link Builder}.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new {@link Builder} by copying all fields from the another {@link
   * AppDeploymentInfo}.
   */
  public static Builder copyFrom(AppDeploymentInfo other) {
    return new Builder()
        .setArtifactId(other.artifactId)
        .setArtifactLocation(other.artifactLocation)
        .setApplicationClass(other.applicationClass)
        .setNamespaceId(other.namespaceId)
        .setAppName(other.appName)
        .setAppVersion(other.appVersion)
        .setConfigString(other.configString)
        .setOwnerPrincipal(other.ownerPrincipal)
        .setUpdateSchedules(other.updateSchedules)
        .setRuntimeInfo(other.runtimeInfo)
        .setChangeDetail(other.changeDetail)
        .setSourceControlMeta(other.sourceControlMeta)
        .setIsUpgrade(other.isUpgrade)
        .setSkipMarkingLatest(other.skipMarkingLatest)
        .setDeployedApplicationSpec(other.deployedApplicationSpec);
  }

  private AppDeploymentInfo(ArtifactId artifactId, Location artifactLocation,
      NamespaceId namespaceId,
      ApplicationClass applicationClass, @Nullable String appName, @Nullable String appVersion,
      @Nullable String configString, @Nullable KerberosPrincipalId ownerPrincipal,
      boolean updateSchedules, @Nullable AppDeploymentRuntimeInfo runtimeInfo,
      @Nullable ChangeDetail changeDetail, @Nullable SourceControlMeta sourceControlMeta,
      boolean isUpgrade, @Nullable ApplicationSpecification deployedApplicationSpec, boolean skipMarkingLatest) {
    this.artifactId = artifactId;
    this.artifactLocation = artifactLocation;
    this.namespaceId = namespaceId;
    this.appName = appName;
    this.appVersion = appVersion;
    this.configString = configString;
    this.ownerPrincipal = ownerPrincipal;
    this.updateSchedules = updateSchedules;
    this.applicationClass = applicationClass;
    this.runtimeInfo = runtimeInfo;
    this.changeDetail = changeDetail;
    this.sourceControlMeta = sourceControlMeta;
    this.isUpgrade = isUpgrade;
    this.skipMarkingLatest = skipMarkingLatest;
    this.deployedApplicationSpec = deployedApplicationSpec;
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
    if (artifactLocation == null) {
      // This shouldn't happen. This is to guard against wrong usage of this class.
      throw new IllegalStateException("Artifact location is not available");
    }
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
   * Returns the configuration string provided for the application deployment or {@code null} if it
   * is not provided.
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

  /**
   * @return the runtime info if the app is deployed at runtime, null if this is the initial deploy
   */
  @Nullable
  public AppDeploymentRuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  /**
   * Returns the change detail {@code null} if it is not provided.
   */
  @Nullable
  public ChangeDetail getChangeDetail() {
    return changeDetail;
  }

  @Nullable
  public SourceControlMeta getSourceControlMeta() {
    return sourceControlMeta;
  }

  /**
   * Returns true if the deploy event type is an upgrade.
   */
  public boolean isUpgrade() {
    return isUpgrade;
  }

  public boolean isSkipMarkingLatest() {
    return skipMarkingLatest;
  }

  /**
   * Returns the previously deployed Application Specification. Will be null for the 1st deployment
   */
  @Nullable
  public ApplicationSpecification getDeployedApplicationSpec() {
    return deployedApplicationSpec;
  }

  /**
   * Builder class for the {@link AppDeploymentInfo}.
   */
  public static final class Builder {

    private ArtifactId artifactId;
    private Location artifactLocation;
    private ApplicationClass applicationClass;
    private NamespaceId namespaceId;
    private String appName;
    private String appVersion;
    private String configString;
    private KerberosPrincipalId ownerPrincipal;
    // The default behavior of update schedules is to update schedule on deployment.
    private boolean updateSchedules = true;
    private AppDeploymentRuntimeInfo runtimeInfo;
    @Nullable
    private ChangeDetail changeDetail;
    @Nullable
    private SourceControlMeta sourceControlMeta;
    @Nullable
    private ApplicationSpecification deployedApplicationSpec;
    private boolean isUpgrade;
    private boolean skipMarkingLatest;

    private Builder() {
      // Only for the builder() method to use
    }

    public Builder setArtifactId(ArtifactId artifactId) {
      this.artifactId = artifactId;
      return this;
    }

    public Builder setArtifactLocation(Location artifactLocation) {
      this.artifactLocation = artifactLocation;
      return this;
    }

    public Builder setApplicationClass(ApplicationClass applicationClass) {
      this.applicationClass = applicationClass;
      return this;
    }

    public Builder setApplicationId(ApplicationId appId) {
      setNamespaceId(appId.getNamespaceId());
      setAppName(appId.getApplication());
      setAppVersion(appId.getVersion());
      return this;
    }

    public Builder setNamespaceId(NamespaceId namespaceId) {
      this.namespaceId = namespaceId;
      return this;
    }

    public Builder setAppName(String appName) {
      this.appName = appName;
      return this;
    }

    public Builder setAppVersion(String appVersion) {
      this.appVersion = appVersion;
      return this;
    }

    public Builder setConfigString(String configString) {
      this.configString = configString;
      return this;
    }

    public Builder setOwnerPrincipal(KerberosPrincipalId ownerPrincipal) {
      this.ownerPrincipal = ownerPrincipal;
      return this;
    }

    public Builder setUpdateSchedules(boolean updateSchedules) {
      this.updateSchedules = updateSchedules;
      return this;
    }

    public Builder setRuntimeInfo(AppDeploymentRuntimeInfo runtimeInfo) {
      this.runtimeInfo = runtimeInfo;
      return this;
    }

    public Builder setChangeDetail(@Nullable ChangeDetail changeDetail) {
      this.changeDetail = changeDetail;
      return this;
    }

    public Builder setSourceControlMeta(@Nullable SourceControlMeta sourceControlMeta) {
      this.sourceControlMeta = sourceControlMeta;
      return this;
    }

    public Builder setIsUpgrade(boolean isUpgrade) {
      this.isUpgrade = isUpgrade;
      return this;
    }

    public Builder setSkipMarkingLatest(boolean skipMarkingLatest) {
      this.skipMarkingLatest = skipMarkingLatest;
      return this;
    }

    public Builder setDeployedApplicationSpec(
        @Nullable ApplicationSpecification deployedApplicationSpec) {
      this.deployedApplicationSpec = deployedApplicationSpec;
      return this;
    }

    public AppDeploymentInfo build() {
      if (artifactId == null) {
        throw new IllegalStateException("Missing artifact ID");
      }
      if (artifactLocation == null) {
        throw new IllegalStateException("Missing artifact location");
      }
      if (namespaceId == null) {
        throw new IllegalStateException("Missing namespace ID");
      }
      if (applicationClass == null) {
        throw new IllegalStateException("Missing application class");
      }
      return new AppDeploymentInfo(artifactId, artifactLocation, namespaceId, applicationClass,
          appName, appVersion, configString, ownerPrincipal, updateSchedules, runtimeInfo,
          changeDetail, sourceControlMeta, isUpgrade, deployedApplicationSpec, skipMarkingLatest);
    }
  }
}
