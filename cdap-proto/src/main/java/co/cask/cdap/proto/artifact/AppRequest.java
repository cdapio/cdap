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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.proto.artifact.preview.PreviewConfig;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Nullable;

/**
 * Request body when creating or updating an app.
 *
 * @param <T> the type of application config
 */
public class AppRequest<T> {
  private final ArtifactSummary artifact;
  private final T config;
  private final PreviewConfig preview;
  @SerializedName("principal")
  private final String ownerPrincipal;

  public AppRequest(ArtifactSummary artifact) {
    this(artifact, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config) {
    this(artifact, config, null, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview) {
    this(artifact, config, preview, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable String ownerPrincipal) {
    this(artifact, config, null, ownerPrincipal);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview,
                    @Nullable String ownerPrincipal) {
    this.artifact = artifact;
    this.config = config;
    this.preview = preview;
    this.ownerPrincipal = ownerPrincipal;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  @Nullable
  public T getConfig() {
    return config;
  }

  @Nullable
  public PreviewConfig getPreview() {
    return preview;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }
}
