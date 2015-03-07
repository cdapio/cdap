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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Represents metadata for namespaces
 */
public final class NamespaceMeta {
  private final String id;
  private final String name;
  private final String description;
  private NamespaceConfig config;

  private NamespaceMeta(String id, String name, String description) {
    this(id, name, description, null);
  }

  private NamespaceMeta(String id, String name, String description, NamespaceConfig config) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.config = config;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public NamespaceConfig getConfig() {
    return config;
  }


  /**
   * Builder used to build {@link NamespaceMeta}
   */
  public static final class Builder {
    private String id;
    private String name;
    private String description;
    private String yarnQueueName;

    public Builder() {
     // No-Op
    }

    public Builder(NamespaceMeta meta) {
      this.id =  meta.getId();
      this.name = meta.getName();
      this.description = meta.getDescription();
      this.yarnQueueName = meta.getConfig().getYarnQueue();
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setDescription(final String description) {
      this.description = description;
      return this;
    }

    public Builder setYarnQueueName(final String yarnQueueName) {
      this.yarnQueueName = yarnQueueName;
      return this;
    }

    public NamespaceMeta build() {
      Preconditions.checkArgument(id != null, "Namespace id cannot be null.");
      if (name == null) {
        name = id;
      }
      if (description == null) {
        description = "";
      }

      if (yarnQueueName == null) {
        yarnQueueName = "";
      }
      return new NamespaceMeta(id, name, description, new NamespaceConfig(yarnQueueName));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return id.equals(((NamespaceMeta) o).id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("name", name)
      .add("description", description)
      .add("config", getConfig())
      .toString();
  }
}
