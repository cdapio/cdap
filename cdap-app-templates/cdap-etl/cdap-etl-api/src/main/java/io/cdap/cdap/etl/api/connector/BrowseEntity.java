/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * An entity that is browsable or samplable, or both.
 * For example, for file connector, this can be file or directory.
 * For database connector, this can be database or table.
 */
public class BrowseEntity {
  private final String name;
  private final String path;
  private final String type;
  private final boolean canSample;
  private final boolean canBrowse;
  private final Collection<BrowseEntityProperty> properties;

  private BrowseEntity(String name, String path, String type, boolean canSample, boolean canBrowse,
                      Collection<BrowseEntityProperty> properties) {
    this.name = name;
    this.path = path;
    this.type = type;
    this.canSample = canSample;
    this.canBrowse = canBrowse;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public String getType() {
    return type;
  }

  public boolean canSample() {
    return canSample;
  }

  public boolean canBrowse() {
    return canBrowse;
  }

  public Collection<BrowseEntityProperty> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BrowseEntity that = (BrowseEntity) o;
    return canSample == that.canSample &&
      canBrowse == that.canBrowse &&
      Objects.equals(name, that.name) &&
      Objects.equals(path, that.path) &&
      Objects.equals(type, that.type) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, path, type, canSample, canBrowse, properties);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder(String name, String path, String type) {
    return new Builder(name, path, type);
  }

  /**
   * Builder for {@link BrowseEntity}
   */
  public static class Builder {
    private String name;
    private String path;
    private String type;
    private boolean canSample;
    private boolean canBrowse;
    private Collection<BrowseEntityProperty> properties;

    public Builder(String name, String path, String type) {
      this.name = name;
      this.type = type;
      this.path = path;
      this.properties = new HashSet<>();
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder canSample(boolean canSample) {
      this.canSample = canSample;
      return this;
    }

    public Builder canBrowse(boolean canBrowse) {
      this.canBrowse = canBrowse;
      return this;
    }

    public Builder setProperties(Collection<BrowseEntityProperty> properties) {
      this.properties.clear();
      this.properties.addAll(properties);
      return this;
    }

    public BrowseEntity build() {
      return new BrowseEntity(name, path, type, canSample, canBrowse, properties);
    }
  }
}
