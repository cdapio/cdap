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
 * Connection information
 */
public class Connection {
  private final String name;
  private final String connectionId;
  private final String connectionType;
  private final String description;
  private final boolean preConfigured;
  private final long createdTimeMillis;
  private final long updatedTimeMillis;
  private final PluginInfo plugin;

  public Connection(String name, String connectionType, String description, boolean preConfigured,
                    long createdTimeMillis, long updatedTimeMillis, PluginInfo plugin) {
    this(name, ConnectionId.getConnectionId(name), connectionType, description, preConfigured, createdTimeMillis,
         updatedTimeMillis, plugin);
  }

  public Connection(String name, String connectionId, String connectionType, String description, boolean preConfigured,
                    long createdTimeMillis, long updatedTimeMillis, PluginInfo plugin) {
    this.name = name;
    this.connectionId = connectionId;
    this.connectionType = connectionType;
    this.description = description;
    this.preConfigured = preConfigured;
    this.createdTimeMillis = createdTimeMillis;
    this.updatedTimeMillis = updatedTimeMillis;
    this.plugin = plugin;
  }

  public String getName() {
    return name;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public String getDescription() {
    return description;
  }

  public boolean isPreConfigured() {
    return preConfigured;
  }

  public long getCreatedTimeMillis() {
    return createdTimeMillis;
  }

  public long getUpdatedTimeMillis() {
    return updatedTimeMillis;
  }

  public PluginInfo getPlugin() {
    return plugin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Connection that = (Connection) o;
    return preConfigured == that.preConfigured &&
      Objects.equals(name, that.name) &&
      Objects.equals(connectionId, that.connectionId) &&
      Objects.equals(connectionType, that.connectionType) &&
      Objects.equals(description, that.description) &&
      Objects.equals(plugin, that.plugin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, connectionId, connectionType, description, preConfigured, plugin);
  }
}
