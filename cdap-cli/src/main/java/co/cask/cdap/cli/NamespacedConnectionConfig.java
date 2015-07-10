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

/**
 * Connection information to a CDAP instance, with namespace.
 */
public class NamespacedConnectionConfig extends ConnectionConfig {

  private final Id.Namespace namespace;

  public NamespacedConnectionConfig(Id.Namespace namespace, String hostname, int port, boolean sslEnabled) {
    super(hostname, port, sslEnabled);
    this.namespace = namespace;
  }

  public NamespacedConnectionConfig(Id.Namespace namespace, ConnectionConfig connectionConfig) {
    super(connectionConfig.getHostname(), connectionConfig.getPort(), connectionConfig.isSSLEnabled());
    this.namespace = namespace;
  }

  public Id.Namespace getNamespace() {
    return namespace;
  }
}
