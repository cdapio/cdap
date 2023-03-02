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

package io.cdap.cdap.etl.proto.v2;

import io.cdap.cdap.etl.proto.connection.PreconfiguredConnectionCreationRequest;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Config for pre configured connections
 */
public class ConnectionConfig {

  private final Set<String> disabledTypes;
  private final Set<PreconfiguredConnectionCreationRequest> connections;
  private final String defaultConnection;

  public ConnectionConfig(Set<String> disabledTypes,
      Set<PreconfiguredConnectionCreationRequest> connections,
      String defaultConnection) {
    this.disabledTypes = disabledTypes;
    this.connections = connections;
    this.defaultConnection = defaultConnection;
  }

  public Set<String> getDisabledTypes() {
    return disabledTypes == null ? Collections.emptySet() : disabledTypes;
  }

  public Set<PreconfiguredConnectionCreationRequest> getConnections() {
    return connections == null ? Collections.emptySet() : connections;
  }

  public String getDefaultConnection() {
    return defaultConnection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectionConfig that = (ConnectionConfig) o;
    return Objects.equals(disabledTypes, that.disabledTypes)
        && Objects.equals(connections, that.connections)
        && Objects.equals(defaultConnection, that.defaultConnection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(disabledTypes, connections, defaultConnection);
  }
}
