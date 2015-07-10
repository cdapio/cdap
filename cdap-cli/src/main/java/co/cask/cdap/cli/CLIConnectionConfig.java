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

package co.cask.cdap.cli;

import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.proto.Id;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * {@link ConnectionConfig} that stores a username when available. Helps keep connectionConfig thread safe.
 */
public class CLIConnectionConfig extends ConnectionConfig {

  private final Id.Namespace namespace;
  private final String username;

  public CLIConnectionConfig(ConnectionConfig connectionConfig, Id.Namespace namespace, @Nullable String username) {
    super(connectionConfig);
    this.namespace = namespace;
    this.username = username;
  }

  public CLIConnectionConfig(CLIConnectionConfig connectionConfig, Id.Namespace namespace) {
    super(connectionConfig);
    this.username = connectionConfig.getUsername();
    this.namespace = namespace;
  }

  public CLIConnectionConfig(Id.Namespace namespace, String hostname, int port, boolean sslEnabled) {
    super(hostname, port, sslEnabled);
    this.namespace = namespace;
    this.username = null;
  }

  public Id.Namespace getNamespace() {
    return namespace;
  }

  public String getUsername() {
    return username;
  }

  @Override
  public URI getURI() {
    return URI.create(String.format("%s://%s%s:%d/%s", super.isSSLEnabled() ? "https" : "http",
                                    username == null ? "" : username + "@",
                                    super.getHostname(), super.getPort(), namespace.getId()));
  }
}
