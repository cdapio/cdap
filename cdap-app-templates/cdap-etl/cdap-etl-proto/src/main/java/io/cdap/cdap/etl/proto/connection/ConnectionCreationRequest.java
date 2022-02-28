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
 * Creation request for a connection
 */
public class ConnectionCreationRequest {
  private final String description;
  // flag indicating whether the creation request should overwrite an existing connection with same connection id
  // but different connection name, i.e, a b and a.b both convert to id a_b
  private final boolean overWrite;
  private final PluginInfo plugin;

  public ConnectionCreationRequest(String description, PluginInfo plugin) {
    this(description, plugin, false);
  }

  public ConnectionCreationRequest(String description, PluginInfo plugin, boolean overWrite) {
    this.description = description;
    this.overWrite = overWrite;
    this.plugin = plugin;
  }

  public String getDescription() {
    return description;
  }

  public PluginInfo getPlugin() {
    return plugin;
  }

  public boolean overWrite() {
    return overWrite;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectionCreationRequest that = (ConnectionCreationRequest) o;
    return overWrite == that.overWrite &&
      Objects.equals(description, that.description) &&
      Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, overWrite, plugin);
  }
}
