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

import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.proto.id.NamespaceId;

import java.net.URI;
import javax.annotation.Nullable;

/**
 * {@link ConnectionConfig} that stores a username when available. Helps keep connectionConfig thread safe.
 */
public class CLIConnectionConfig extends ConnectionConfig {

  private final NamespaceId namespace;
  private final String username;

  private static final String CDF_API_PREFIX = "/api";

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

  public CLIConnectionConfig(NamespaceId namespace, String hostname, int port, boolean sslEnabled) {
    super(hostname, port, sslEnabled);
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
    StringBuilder builder = new StringBuilder(String.format("%s://%s%s", super.isSSLEnabled() ? "https" : "http",
        username == null ? "" : username + "@", super.getHostname()));
    if (super.getPort() != -1) {
      builder.append(String.format(":%s", super.getPort()));
    }

    if (super.getHostname().contains(InstanceURIParser.CDF_SUFFIX)) {
      builder.append(CDF_API_PREFIX);
    } else {
      builder.append(String.format("/%s", namespace.getNamespace()));
    }
    return URI.create(builder.toString());
  }

  @Override
  public URI resolveURI(String path) {
    if (super.getHostname().contains(InstanceURIParser.CDF_SUFFIX)) {
      return getURI().resolve(String.format("%s/%s", CDF_API_PREFIX, path));
    }
    return getURI().resolve(String.format("/%s", path));
  }

  @Override
  public URI resolveURI(String apiVersion, String path) {
    if (super.getHostname().contains(InstanceURIParser.CDF_SUFFIX)) {
      return getURI().resolve(String.format("%s/%s/%s", CDF_API_PREFIX, apiVersion, path));
    }
    return getURI().resolve(String.format("/%s/%s", apiVersion, path));
  }

  @Override
  public URI resolveNamespacedURI(NamespaceId namespace, String apiVersion, String path) {
    if (super.getHostname().contains(InstanceURIParser.CDF_SUFFIX)) {
      return getURI().resolve(String.format("%s/%s/namespaces/%s/%s", CDF_API_PREFIX, apiVersion,
          namespace.getNamespace(), path));
    }
    return getURI().resolve(String.format("/%s/namespaces/%s/%s", apiVersion, namespace.getNamespace(), path));
  }
}
