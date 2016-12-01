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
package co.cask.cdap.client.config;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import javax.annotation.Nullable;

/**
 * Connection information to a CDAP instance.
 */
public class ConnectionConfig {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionConfig.class);
  private static final CConfiguration CONF = CConfiguration.create();
  private static final int DEFAULT_PORT = CONF.getInt(Constants.Router.ROUTER_PORT);
  private static final int DEFAULT_SSL_PORT = CONF.getInt(Constants.Router.ROUTER_SSL_PORT);
  private static final boolean DEFAULT_SSL_ENABLED = CONF.getBoolean(Constants.Security.SSL_ENABLED, false);
  private static final String DEFAULT_HOST = tryResolveAddress(CONF.get(Constants.Router.ADDRESS));

  private static String tryResolveAddress(String addressString) {
    try {
      InetAddress address = InetAddress.getByName(addressString);
      if (address.isAnyLocalAddress()) {
        return InetAddress.getLocalHost().getHostName();
      }
    } catch (UnknownHostException e) {
      LOG.warn("Unable to resolve address", e);
    }
    return addressString;
  }

  public static final ConnectionConfig DEFAULT = ConnectionConfig.builder().build();

  private final String hostname;
  private final int port;
  private final boolean sslEnabled;

  public ConnectionConfig(ConnectionConfig config) {
    this(config.getHostname(), config.getPort(), config.isSSLEnabled());
  }

  public ConnectionConfig(String hostname, int port, boolean sslEnabled) {
    Preconditions.checkArgument(hostname != null && !hostname.isEmpty(), "hostname cannot be empty");
    this.hostname = hostname;
    this.port = port;
    this.sslEnabled = sslEnabled;
  }

  public URI getURI() {
    return URI.create(String.format("%s://%s:%d", sslEnabled ? "https" : "http", hostname, port));
  }

  public URI resolveURI(String path) {
    return getURI().resolve(String.format("/%s", path));
  }

  public URI resolveURI(String apiVersion, String path) {
    return getURI().resolve(String.format("/%s/%s", apiVersion, path));
  }

  public URI resolveNamespacedURI(NamespaceId namespace, String apiVersion, String path) {
    return getURI().resolve(String.format("/%s/namespaces/%s/%s", apiVersion, namespace.getNamespace(), path));
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public boolean isSSLEnabled() {
    return sslEnabled;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hostname, port, sslEnabled);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ConnectionConfig other = (ConnectionConfig) obj;
    return Objects.equal(this.hostname, other.hostname) &&
      Objects.equal(this.port, other.port) &&
      Objects.equal(this.sslEnabled, other.sslEnabled);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hostname", hostname)
      .add("port", port)
      .add("sslEnabled", sslEnabled)
      .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(ConnectionConfig connectionConfig) {
    return new Builder(connectionConfig);
  }

  /**
   * Builder for {@link ConnectionConfig}.
   */
  public static class Builder {
    private String hostname = DEFAULT_HOST;
    private Integer port = null;
    private boolean sslEnabled = DEFAULT_SSL_ENABLED;

    public Builder() {
    }

    public Builder(ConnectionConfig connectionConfig) {
      this.hostname = connectionConfig.hostname;
      this.port = connectionConfig.port;
      this.sslEnabled = connectionConfig.sslEnabled;
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    /**
     * @param port connection port - use null if you want to use the default non-SSL or SSL port,
     *             depending on sslEnabled
     * @return this
     */
    public Builder setPort(@Nullable Integer port) {
      this.port = port;
      return this;
    }

    public Builder setSSLEnabled(boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      return this;
    }

    public ConnectionConfig build() {
      if (port == null) {
        if (sslEnabled) {
          port = DEFAULT_SSL_PORT;
        } else {
          port = DEFAULT_PORT;
        }
      }
      return new ConnectionConfig(hostname, port, sslEnabled);
    }
  }
}
