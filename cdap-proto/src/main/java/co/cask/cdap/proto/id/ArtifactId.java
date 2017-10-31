/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies an artifact.
 */
public class ArtifactId extends NamespacedEntityId implements ParentedId<NamespaceId> {
  private final String artifact;
  private final String version;
  private transient Integer hashCode;

  public ArtifactId(String namespace, String artifact, String version) {
    super(namespace, EntityType.ARTIFACT);
    if (artifact == null) {
      throw new NullPointerException("Artifact ID cannot be null.");
    }
    if (version == null) {
      throw new NullPointerException("Version cannot be null.");
    }
    ensureValidArtifactId("artifact", artifact);
    ArtifactVersion artifactVersion = new ArtifactVersion(version);
    if (artifactVersion.getVersion() == null) {
      throw new IllegalArgumentException("Invalid artifact version " + version);
    }
    this.artifact = artifact;
    this.version = version;
  }

  /**
   * Parses a string expected to be of the form {name}-{version}.jar into an {@link ArtifactId},
   * where name is a valid id and version is of the form expected by {@link ArtifactVersion}.
   *
   * @param namespace the namespace to use
   * @param fileName the string to parse
   * @throws IllegalArgumentException if the string is not in the expected format
   */
  public ArtifactId(String namespace, String fileName) {
    super(namespace, EntityType.ARTIFACT);
    if (!fileName.endsWith(".jar")) {
      throw new IllegalArgumentException(String.format("Artifact name '%s' does not end in .jar", fileName));
    }

    // strip '.jar' from the filename
    fileName = fileName.substring(0, fileName.length() - ".jar".length());

    // true means try and match version as the end of the string
    ArtifactVersion artifactVersion = new ArtifactVersion(fileName, true);
    String rawVersion = artifactVersion.getVersion();
    // this happens if it could not parse the version
    if (rawVersion == null) {
      throw new IllegalArgumentException(
        String.format("Artifact name '%s' is not of the form {name}-{version}.jar", fileName));
    }

    // filename should be {name}-{version}.  Strip -{version} from it to get artifact name
    this.artifact = fileName.substring(0, fileName.length() - rawVersion.length() - 1);
    this.version = rawVersion;
  }

  public String getArtifact() {
    return artifact;
  }

  @Override
  public String getEntityName() {
    return getArtifact();
  }

  public String getVersion() {
    return version;
  }

  @Override
  public NamespaceId getParent() {
    return new NamespaceId(namespace);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ArtifactId that = (ArtifactId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(artifact, that.artifact) &&
      Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, artifact, version);
    }
    return hashCode;
  }

  @Override
  public Id.Artifact toId() {
    return Id.Artifact.from(Id.Namespace.from(namespace), artifact, version);
  }

  @SuppressWarnings("unused")
  public static ArtifactId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ArtifactId(
      next(iterator, "namespace"), next(iterator, "artifact"),
      remaining(iterator, "version"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, artifact, version));
  }

  public static ArtifactId fromString(String string) {
    return EntityId.fromString(string, ArtifactId.class);
  }
}
