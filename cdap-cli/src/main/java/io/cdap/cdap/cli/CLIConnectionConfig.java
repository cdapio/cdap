/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.URI;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * {@link ConnectionConfig} that stores a username when available. Helps keep connectionConfig thread safe.
 */
public class CLIConnectionConfig extends ConnectionConfig {

  private final NamespaceId namespace;
  private final String username;

  public CLIConnectionConfig(ConnectionConfig connectionConfig, NamespaceId namespace, @Nullable String username) {
    super(connectionConfig);
    this.namespace = namespace;
    this.username = username;
  }

  public CLIConnectionConfig(CLIConnectionConfig connectionConfig, NamespaceId namespace) {
    super(connectionConfig);
    this.username = connectionConfig.getUsername();
    this.namespace = namespace;
  }

  public CLIConnectionConfig(NamespaceId namespace, String hostname, Integer port, boolean sslEnabled) {
    this(namespace, hostname, port, sslEnabled, null);
  }

  public CLIConnectionConfig(NamespaceId namespace, String hostname, Integer port, boolean sslEnabled,
                             @Nullable String apiPath) {
    super(hostname, port, sslEnabled, apiPath);
    this.namespace = namespace;
    this.username = null;
  }

  public NamespaceId getNamespace() {
    return namespace;
  }

  public String getUsername() {
    return username;
  }

  @Override
  public URI getURI() {
    return URI.create(String.format("%s://%s%s/%s%s", super.isSSLEnabled() ? "https" : "http",
                                    (username == null || username.isEmpty()) ? "" : username + "@",
                                    getFullHost(), getApiPath() == null ? "" : getApiPath(),
                                    namespace.getNamespace()));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CLIConnectionConfig other = (CLIConnectionConfig) obj;
    return Objects.equals(this.namespace, other.namespace)
      && Objects.equals(this.username, other.username)
      && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), namespace, username);
  }
}
