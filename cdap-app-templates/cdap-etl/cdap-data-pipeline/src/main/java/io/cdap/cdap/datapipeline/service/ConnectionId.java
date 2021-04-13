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

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Objects;

/**
 * 
 */
public class ConnectionId {
  private final NamespaceId namespaceId;
  private final String connection;

  public ConnectionId(NamespaceId namespaceId, String connection) {
    this.namespaceId = namespaceId;
    this.connection = connection;
  }

  public NamespaceId getNamespaceId() {
    return namespaceId;
  }

  public String getConnection() {
    return connection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConnectionId that = (ConnectionId) o;
    return Objects.equals(namespaceId, that.namespaceId) &&
      Objects.equals(connection, that.connection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceId, connection);
  }
}
