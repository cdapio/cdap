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

import co.cask.cdap.proto.artifact.ArtifactSummary;

import java.util.Objects;

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

  public ApplicationRecord(ArtifactSummary artifact, String name, String description) {
    this.type = "App";
    this.artifact = artifact;
    this.name = name;
    this.description = description;
    this.version = artifact.getVersion();
    this.id = name;
  }

  @Deprecated
  public ApplicationRecord(String name, String version, String description) {
    this("App", name, name, version, description);
  }

  @Deprecated
  public ApplicationRecord(String type, String id, String name, String version, String description) {
    this.type = type;
    this.id = id;
    this.name = name;
    this.version = version;
    this.description = description;
    this.artifact = null;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  /**
   * @deprecated use {@link #getArtifact()} instead
   * @return the version of the artifact used to create the application
   */
  @Deprecated
  public String getVersion() {
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
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, version, description, artifact);
  }

  @Override
  public String toString() {
    return "ApplicationRecord{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", description='" + description + '\'' +
      ", artifact=" + artifact +
      '}';
  }
}
