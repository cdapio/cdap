/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.gson.annotations.SerializedName;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents item in the list from /apps
 */
public class ApplicationRecord {
  private final String type;
  private final String id;
  private final String name;
  private final String version;
  private final String description;
  private final ArtifactSummary artifact;
  @SerializedName("principal")
  private final String ownerPrincipal;


  public ApplicationRecord(ArtifactSummary artifact, ApplicationId appId, String description) {
    this(artifact, appId, description, null);
  }

  public ApplicationRecord(ArtifactSummary artifact, ApplicationId appId, String description,
                           @Nullable String ownerPrincipal) {
    this.type = "App";
    this.artifact = artifact;
    this.name = appId.getApplication();
    this.description = description;
    this.version = appId.getVersion();
    this.id = appId.getApplication();
    this.ownerPrincipal = ownerPrincipal;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  public String getAppVersion() {
    return version;
  }

  public String getType() {
    return type;
  }

  @Deprecated
  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ApplicationRecord that = (ApplicationRecord) o;

    return Objects.equals(type, that.type) &&
      Objects.equals(name, that.name) &&
      Objects.equals(version, that.version) &&
      Objects.equals(description, that.description) &&
      Objects.equals(artifact, that.artifact) &&
      Objects.equals(ownerPrincipal, that.ownerPrincipal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, version, description, artifact, ownerPrincipal);
  }

  @Override
  public String toString() {
    return "ApplicationRecord{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", description='" + description + '\'' +
      ", artifact=" + artifact +
      ", ownerPrincipal=" + ownerPrincipal +
      '}';
  }
}
