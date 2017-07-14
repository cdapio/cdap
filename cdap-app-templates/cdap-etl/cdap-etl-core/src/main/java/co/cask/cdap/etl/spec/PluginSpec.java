/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spec;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Objects;

/**
 * Specification for a plugin.
 *
 * This is like an {@link ETLPlugin}, but has additional attributes calculated at configure time of the application.
 * The spec contains the artifact selected for the plugin.
 *
 * Implements Externalizable since ArtifactId is a CDAP class and is not Serializable.
 */
public class PluginSpec implements Externalizable {
  private String type;
  private String name;
  private Map<String, String> properties;
  private ArtifactId artifact;

  public PluginSpec() {
    // required for deserialization
  }

  public PluginSpec(String type, String name, Map<String, String> properties, ArtifactId artifact) {
    this.type = type;
    this.name = name;
    this.properties = ImmutableMap.copyOf(properties);
    this.artifact = artifact;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ArtifactId getArtifact() {
    return artifact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginSpec that = (PluginSpec) o;

    return Objects.equals(type, that.type) &&
      Objects.equals(name, that.name) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(artifact, that.artifact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, properties, artifact);
  }

  @Override
  public String toString() {
    return "PluginSpec{" +
      "type='" + type + '\'' +
      ", name='" + name + '\'' +
      ", properties=" + properties +
      ", artifact=" + artifact +
      '}';
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(type);
    out.writeUTF(name);
    out.writeObject(properties);
    out.writeUTF(artifact.getName());
    out.writeUTF(artifact.getVersion().getVersion());
    out.writeUTF(artifact.getScope().name());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    type = in.readUTF();
    name = in.readUTF();
    properties = (Map<String, String>) in.readObject();
    String artifactName = in.readUTF();
    ArtifactVersion artifactVersion = new ArtifactVersion(in.readUTF());
    ArtifactScope artifactScope = ArtifactScope.valueOf(in.readUTF());
    artifact = new ArtifactId(artifactName, artifactVersion, artifactScope);
  }
}
