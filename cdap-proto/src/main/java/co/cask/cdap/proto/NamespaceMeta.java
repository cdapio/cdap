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

/**
 * Represents metadata for namespaces
 */
public final class NamespaceMeta {

  private final String name;
  private final String description;
  private NamespaceConfig config;

  private NamespaceMeta(String name, String description, NamespaceConfig config) {
    this.name = name;
    this.description = description;
    this.config = config;
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
    private String name;
    private String description;
    private String schedulerQueueName;

    public Builder() {
     // No-Op
    }

    public Builder(NamespaceMeta meta) {
      this.name = meta.getName();
      this.description = meta.getDescription();
      this.schedulerQueueName = meta.getConfig().getSchedulerQueueName();
    }

    public Builder setName(final Id.Namespace id) {
      this.name = id.getId();
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

    public Builder setSchedulerQueueName(final String schedulerQueueName) {
      this.schedulerQueueName = schedulerQueueName;
      return this;
    }

    public NamespaceMeta build() {
      Preconditions.checkArgument(name != null, "Namespace id cannot be null.");
      if (description == null) {
        description = "";
      }

      if (schedulerQueueName == null) {
        schedulerQueueName = "";
      }
      return new NamespaceMeta(name, description, new NamespaceConfig(schedulerQueueName));
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
    NamespaceMeta other = (NamespaceMeta) o;
    return Objects.equal(name, other.name)
      && Objects.equal(description, other.description)
      && Objects.equal(config, other.config);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, description, config);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("description", description)
      .add("config", getConfig())
      .toString();
  }
}
