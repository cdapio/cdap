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

import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
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
public final class ConnectionConfig {

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

  public static final ConnectionConfig DEFAULT = ConnectionConfig.builder().unAuthenticatedConnection().get().build();

  private final String hostname;
  private final int port;
  private final boolean sslEnabled;
  private final Id.Namespace namespace;
  private final String username;

  public ConnectionConfig(Id.Namespace namespace, String hostname, int port, boolean sslEnabled, String userName) {
    Preconditions.checkArgument(namespace != null, "namespace cannot be empty");
    Preconditions.checkArgument(hostname != null && !hostname.isEmpty(), "hostname cannot be empty");
    this.namespace = namespace;
    this.hostname = hostname;
    this.port = port;
    this.sslEnabled = sslEnabled;
    this.username = userName;
  }

  public ConnectionConfig(Id.Namespace namespace, String hostname, int port, boolean sslEnabled) {
    Preconditions.checkArgument(namespace != null, "namespace cannot be empty");
    Preconditions.checkArgument(hostname != null && !hostname.isEmpty(), "hostname cannot be empty");
    this.namespace = namespace;
    this.hostname = hostname;
    this.port = port;
    this.sslEnabled = sslEnabled;
    this.username = "";
  }

  public URI getURI() {
    if (username.equals("")) {
      return URI.create(String.format("%s://%s:%d", sslEnabled ? "https" : "http", hostname, port));
    } else {
      return URI.create(String.format("%s://%s%s:%d", sslEnabled ? "https" : "http", username,
                                      hostname, port));
    }
  }

  public URI resolveURI(String path) {
    return getURI().resolve(String.format("/%s", path));
  }

  public URI resolveURI(String apiVersion, String path) {
    return getURI().resolve(String.format("/%s/%s", apiVersion, path));
  }

  public URI resolveNamespacedURI(String apiVersion, String path) {
    return getURI().resolve(String.format("/%s/namespaces/%s/%s", apiVersion, namespace.getId(), path));
  }

  public Id.Namespace getNamespace() {
    return namespace;
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
      Objects.equal(this.sslEnabled, other.sslEnabled) &&
      Objects.equal(this.namespace, other.namespace) &&
      Objects.equal(this.username, other.username);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hostname", hostname)
      .add("port", port)
      .add("sslEnabled", sslEnabled)
      .add("namespace", namespace)
      .add("username", username)
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
    private Id.Namespace namespace = Constants.DEFAULT_NAMESPACE_ID;
    private String username = "";

    public Builder() {
    }

    public Builder(ConnectionConfig connectionConfig) {
      this.hostname = connectionConfig.hostname;
      this.port = connectionConfig.port;
      this.sslEnabled = connectionConfig.sslEnabled;
      this.namespace = connectionConfig.namespace;
      this.username = connectionConfig.username;
    }

    public AuthenticatedConnection authenticatedConnection() {
      return new AuthenticatedConnection();
    }

    public UnAuthenticatedConnection unAuthenticatedConnection() {
      return new UnAuthenticatedConnection();
    }

    /**
     *
     */
    public final class AuthenticatedConnection {

      private AuthenticatedConnection() {}

      public InternalBuilder userName(String userName) {
        Builder.this.username = userName + "@";
        return new InternalBuilder();
      }

    }

    /**
     *
     */
    public final class InternalBuilder {
      public InternalBuilder setNamespace(Id.Namespace namespace) {
        Builder.this.namespace = namespace;
        return new InternalBuilder();
      }

      public InternalBuilder setHostname(String hostname) {
        Builder.this.hostname = hostname;
        return new InternalBuilder();
      }

      public InternalBuilder setPort(@Nullable Integer port) {
        Builder.this.port = port;
        return new InternalBuilder();
      }

      public InternalBuilder setSSLEnabled(boolean sslEnabled) {
        Builder.this.sslEnabled = sslEnabled;
        return new InternalBuilder();
      }

      public ConnectionConfig build() {
        if (port == null) {
          if (sslEnabled) {
            port = DEFAULT_SSL_PORT;
          } else {
            port = DEFAULT_PORT;
          }
        }
        return new ConnectionConfig(namespace, hostname, port, sslEnabled, username);
      }

    }

    /**
     *
     */
    public final class UnAuthenticatedConnection {
      private UnAuthenticatedConnection() {}

      public InternalBuilder get() {
        return new InternalBuilder();
      }
    }
  }
}
