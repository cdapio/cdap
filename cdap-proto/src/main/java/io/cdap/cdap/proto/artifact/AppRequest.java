/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.proto.artifact;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.proto.artifact.preview.PreviewConfig;

import javax.annotation.Nullable;

/**
 * Request body when creating or updating an app.
 *
 * @param <T> the type of application config
 */
public class AppRequest<T> {
  private final ArtifactSummary artifact;
  private final T config;
  private final T configuration;
  private final PreviewConfig preview;
  @SerializedName("principal")
  private final String ownerPrincipal;
  @SerializedName("app.deploy.update.schedules")
  private final Boolean updateSchedules;


  public AppRequest(ArtifactSummary artifact) {
    this(artifact, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config) {
    this(artifact, config, null, null, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview) {
    this(artifact, config, preview, null, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable String ownerPrincipal) {
    this(artifact, config, null, ownerPrincipal, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview,
                    @Nullable String ownerPrincipal, @Nullable Boolean updateSchedules) {
    this(artifact, config, preview, ownerPrincipal, updateSchedules, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview,
                    @Nullable String ownerPrincipal, @Nullable Boolean updateSchedules, @Nullable T configuration) {
    this.artifact = artifact;
    this.config = config;
    this.preview = preview;
    this.ownerPrincipal = ownerPrincipal;
    this.updateSchedules = updateSchedules;
    this.configuration = configuration;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  @Nullable
  public T getConfig() {
    return config != null ? config : configuration;
  }

  @Nullable
  public PreviewConfig getPreview() {
    return preview;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  @Nullable
  public Boolean canUpdateSchedules() {
    return updateSchedules;
  }

  /**
   * Validate the app request contains all required information. Should be called when this object is created through
   * deserializing user input.
   *
   * @throws IllegalArgumentException if the request is invalid
   */
  public void validate() {
    if (artifact == null) {
      throw new IllegalArgumentException("An artifact must be specified to create an application.");
    }
    artifact.validate();
  }
}
