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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
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

  private static final JsonParser JSON_PARSER = new JsonParser();

  private final ArtifactSummary artifact;
  private final T config;
  private final T configuration;
  @Nullable
  private final ChangeSummary change;
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
      @Nullable String ownerPrincipal, @Nullable Boolean updateSchedules,
      @Nullable T configuration) {
    this(artifact, config, preview, ownerPrincipal, updateSchedules, configuration, null);
  }

  public AppRequest(ArtifactSummary artifact, @Nullable T config, @Nullable PreviewConfig preview,
      @Nullable String ownerPrincipal, @Nullable Boolean updateSchedules, @Nullable T configuration,
      @Nullable ChangeSummary change) {
    this.artifact = artifact;
    this.config = config;
    this.preview = preview;
    this.ownerPrincipal = ownerPrincipal;
    this.updateSchedules = updateSchedules;
    this.configuration = configuration;
    this.change = change;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  @Nullable
  public T getConfig() {
    return config != null ? config : configuration;
  }

  @Nullable
  public ChangeSummary getChange() {
    return change;
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
   * Validate the app request contains all required information. Should be called when this object
   * is created through deserializing user input.
   *
   * @throws IllegalArgumentException if the request is invalid
   */
  public void validate() {
    if (artifact == null) {
      throw new IllegalArgumentException("An artifact must be specified to create an application.");
    }
    artifact.validate();
  }

  /**
   * Returns true if the two configurations are semantically identical JSON strings.
   */
  public static boolean areConfigsEqual(@Nullable String config1, @Nullable String config2) {
    if (config1 == null || config2 == null) {
      return false;
    }


    String trim1 = config1.trim();
    String trim2 = config2.trim();
    if (trim1.isEmpty() || trim2.isEmpty()) {
      return false;
    }

    if (trim1.equals(trim2)){
      return true;
    }

    try {
      JsonElement element1 = JSON_PARSER.parse(trim1);
      JsonElement element2 = JSON_PARSER.parse(trim2);
      return element1.equals(element2);
    } catch (JsonSyntaxException e) {
      return false;
    }
  }
}
