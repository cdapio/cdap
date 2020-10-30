/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.apache.twill.filesystem.Location;

import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Represents information of an application that is undergoing deployment.
 */
public class ApplicationDeployable {

  private final ArtifactId artifactId;
  private final Location artifactLocation;
  private final ApplicationId applicationId;
  private final ApplicationClass applicationClass;
  private final ApplicationSpecification specification;
  private final ApplicationSpecification existingAppSpec;
  private final ApplicationDeployScope applicationDeployScope;
  private final Collection<StructuredTableSpecification> systemTables;
  @SerializedName("principal")
  private final KerberosPrincipalId ownerPrincipal;
  @SerializedName("update-schedules")
  private final boolean updateSchedules;

  public ApplicationDeployable(ArtifactId artifactId, Location artifactLocation,
                               ApplicationId applicationId, ApplicationSpecification specification,
                               @Nullable ApplicationSpecification existingAppSpec,
                               ApplicationDeployScope applicationDeployScope,
                               ApplicationClass applicationClass) {
    this(artifactId, artifactLocation, applicationId, specification, existingAppSpec, applicationDeployScope,
         applicationClass, null, true, Collections.emptyList());
  }

  public ApplicationDeployable(ArtifactId artifactId, Location artifactLocation,
                               ApplicationId applicationId, ApplicationSpecification specification,
                               @Nullable ApplicationSpecification existingAppSpec,
                               ApplicationDeployScope applicationDeployScope,
                               ApplicationClass applicationClass,
                               @Nullable KerberosPrincipalId ownerPrincipal,
                               boolean updateSchedules,
                               Collection<StructuredTableSpecification> systemTables) {
    this.artifactId = artifactId;
    this.artifactLocation = artifactLocation;
    this.applicationId = applicationId;
    this.specification = specification;
    this.existingAppSpec = existingAppSpec;
    this.applicationDeployScope = applicationDeployScope;
    this.ownerPrincipal = ownerPrincipal;
    this.updateSchedules = updateSchedules;
    this.systemTables = systemTables;
    this.applicationClass = applicationClass;
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
   * Returns the {@link ApplicationId} of the application.
   */
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  /**
   *
   * @return {@link io.cdap.cdap.api.artifact.ApplicationClass} of the Application
   */
  public ApplicationClass getApplicationClass() {
    return applicationClass;
  }

  /**
   * Returns the {@link ApplicationSpecification} of the application.
   */
  public ApplicationSpecification getSpecification() {
    return specification;
  }

  /**
   * Returns the {@link ApplicationSpecification} of the older version of the application or {@code null} if
   * it doesn't exist.
   */
  @Nullable
  public ApplicationSpecification getExistingAppSpec() {
    return existingAppSpec;
  }

  /**
   * Returns the {@link ApplicationDeployScope} of this application is deploying to.
   */
  public ApplicationDeployScope getApplicationDeployScope() {
    return applicationDeployScope;
  }

  /**
   * @return the {@link KerberosPrincipalId} of the application owner
   */
  @Nullable
  public KerberosPrincipalId getOwnerPrincipal() {
    return ownerPrincipal;
  }

  public Collection<StructuredTableSpecification> getSystemTables() {
    return systemTables;
  }

  /**
   * @return true if we can update the schedules of the app
   */
  public boolean canUpdateSchedules() {
    return updateSchedules;
  }
}
