/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.objects;

import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import org.apache.twill.filesystem.Location;

/**
 * Data transfer object (DTO) that defines an artifact
 */
public class Artifact {

  private final String name;
  private final String version;
  private final String scope;
  private final String namespace;
  private final Location location;
  // TODO this should not be an object, but rather a sort of key to access metadata
  private final ArtifactMeta meta;

  private Artifact(Builder builder) {
    name = builder.name;
    version = builder.version;
    scope = builder.scope;
    namespace = builder.namespace;
    location = builder.location;
    meta = builder.meta;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public String getScope() {
    return scope;
  }

  public String getNamespace() {
    return namespace;
  }

  public Location getLocation() {
    return location;
  }

  public ArtifactMeta getMeta() {
    return meta;
  }

  /**
   * Base implementation of a Builder for an Artifact
   */
  public static class Builder {
    // TODO check which ones are required
    private String name;
    private String version;
    private String scope;
    private String namespace;
    private Location location;
    private ArtifactMeta meta;

    public Builder name(String name) {
      this.name = name;

      return this;
    }

    public Builder version(String version) {
      this.version = version;

      return this;
    }

    public Builder scope(String scope) {
      this.scope = scope;

      return this;
    }

    public Builder namespace(String namespace) {
      this.namespace = namespace;

      return this;
    }

    public Builder location(Location location) {
      this.location = location;

      return this;
    }

    public Builder meta(ArtifactMeta meta) {
      this.meta = meta;

      return this;
    }

    public Artifact build() {
      return new Artifact(this);
    }
  }

}
