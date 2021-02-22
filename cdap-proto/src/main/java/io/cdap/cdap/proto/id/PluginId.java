/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.proto.id;

import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.element.EntityType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a plugin.
 */
public class PluginId extends NamespacedEntityId implements ParentedId<ArtifactId> {
  private final String artifact;
  private final String version;
  private final String plugin;
  private final String type;
  private transient Integer hashCode;

  public PluginId(String namespace, String artifact, String version, String plugin, String type) {
    super(namespace, EntityType.PLUGIN);
    this.artifact = Objects.requireNonNull(artifact, "Artifact ID cannot be null.");
    this.version = Objects.requireNonNull(version, "Version cannot be null.");
    this.plugin = Objects.requireNonNull(plugin, "Plugin cannot be null.");
    this.type = Objects.requireNonNull(type, "Type cannot be null.");

    ensureValidArtifactId("artifact", artifact);
    ArtifactVersion artifactVersion = new ArtifactVersion(version);
    if (artifactVersion.getVersion() == null) {
      throw new IllegalArgumentException("Invalid artifact version " + version);
    }
  }

  public String getArtifact() {
    return artifact;
  }

  public String getPlugin() {
    return plugin;
  }

  public String getType() {
    return type;
  }

  @Override
  public String getEntityName() {
    return getPlugin();
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, namespace)
      .append(MetadataEntity.ARTIFACT, artifact)
      .append(MetadataEntity.VERSION, version)
      .append(MetadataEntity.TYPE, type)
      .appendAsType(MetadataEntity.PLUGIN, plugin)
      .build();
  }

  public String getVersion() {
    return version;
  }

  @Override
  public ArtifactId getParent() {
    return new ArtifactId(namespace, artifact, version);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    PluginId that = (PluginId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(artifact, that.artifact) &&
      Objects.equals(version, that.version) &&
      Objects.equals(type, that.type) &&
      Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, artifact, version, type, plugin);
    }
    return hashCode;
  }

  @SuppressWarnings("unused")
  public static PluginId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new PluginId(
      next(iterator, "namespace"), next(iterator, "artifact"),
      next(iterator, "version"), next(iterator, "type"), remaining(iterator, "plugin"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, artifact, version, type, plugin));
  }

  public static PluginId fromString(String string) {
    return EntityId.fromString(string, PluginId.class);
  }
}
