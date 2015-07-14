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

package co.cask.cdap.proto.artifact;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.artifact.ArtifactClasses;

import java.util.Objects;

/**
 * Represents an artifact returned by /artifacts/{artifact-name}/versions/{artifact-version}.
 */
@Beta
public class ArtifactInfo {
  private final String name;
  private final String version;
  private final boolean isSystem;
  private final ArtifactClasses classes;

  public ArtifactInfo(String name, String version, boolean isSystem, ArtifactClasses classes) {
    this.name = name;
    this.version = version;
    this.isSystem = isSystem;
    this.classes = classes;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public boolean isSystem() {
    return isSystem;
  }

  public ArtifactClasses getClasses() {
    return classes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArtifactInfo that = (ArtifactInfo) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(version, that.version) &&
      isSystem == that.isSystem &&
      Objects.equals(classes, that.classes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, version, isSystem, classes);
  }

  @Override
  public String toString() {
    return "ArtifactInfo{" +
      "name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", isSystem=" + isSystem +
      ", classes=" + classes +
      '}';
  }
}
