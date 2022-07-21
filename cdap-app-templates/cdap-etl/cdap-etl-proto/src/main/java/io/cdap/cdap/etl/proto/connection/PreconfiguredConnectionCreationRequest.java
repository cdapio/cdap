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

package io.cdap.cdap.etl.proto.connection;

import java.util.Objects;

/**
 * Creation reqeuest for preconfigured connection with name and namespace
 */
public class PreconfiguredConnectionCreationRequest extends ConnectionCreationRequest {
  private final String namespace;
  private final String name;

  public PreconfiguredConnectionCreationRequest(String description, PluginInfo plugin, String namespace, String name) {
    super(description, plugin);
    this.namespace = namespace;
    this.name = name;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    PreconfiguredConnectionCreationRequest that = (PreconfiguredConnectionCreationRequest) o;
    return Objects.equals(namespace, that.namespace) &&
             Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, name);
  }
}
