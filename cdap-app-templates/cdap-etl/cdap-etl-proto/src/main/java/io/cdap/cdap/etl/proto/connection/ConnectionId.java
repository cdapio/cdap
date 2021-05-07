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

import io.cdap.cdap.api.NamespaceSummary;
import jdk.nashorn.internal.runtime.regexp.RegExp;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Connection id to identify a connection
 */
public class ConnectionId {
  private static final Pattern MACRO_CHARS = Pattern.compile("[${}()]");
  private static final String REGEX = "[^a-zA-Z0-9_]";
  private static final String REPLACED_CHAR = "_";
  private final NamespaceSummary namespace;
  private final String connection;
  private final String connectionId;

  public ConnectionId(NamespaceSummary namespace, String connectionName) {
    this.namespace = namespace;
    this.connection = connectionName;
    this.connectionId = getConnectionId(connectionName);
  }

  public NamespaceSummary getNamespace() {
    return namespace;
  }

  public String getConnection() {
    return connection;
  }

  public String getConnectionId() {
    return connectionId;
  }

  /**
   * Get connection id for the given connection name. The connection name should not contain any characters specific
   * to macro evaluation.
   * Connection id will be lower case and only contains a-z, 0-9 and underscore.
   * The length requirement is <=50, since the metadata tag has a restriction of 50 characters.
   *
   * @param name name of the connection.
   * @return connection id.
   */
  public static String getConnectionId(String name) {
    if (MACRO_CHARS.matcher(name).find()) {
      throw new ConnectionBadRequestException(String.format("The connection name %s should not contain characters " +
                                                              "'$', '{', '}', '(', ')'.", name));
    }

    name = name.trim();

    // Filtering unwanted characters
    name = name.replaceAll(REGEX, REPLACED_CHAR);
    if (name.length() > 50) {
      name = name.substring(0, 50);
    }
    if (name.isEmpty()) {
      throw new ConnectionBadRequestException(
        String.format("The connection name %s should contain at least one alphanumeric character or '_'.", name));
    }
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

    ConnectionId that = (ConnectionId) o;
    return Objects.equals(namespace, that.namespace) &&
      Objects.equals(connection, that.connection) &&
      Objects.equals(connectionId, that.connectionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, connection, connectionId);
  }
}
