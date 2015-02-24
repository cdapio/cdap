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
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.net.URI;

/**
 * Connection-specific configuration.
 */
public final class ConnectionConfig {

  private static final CConfiguration CONF = CConfiguration.create();

  private static final String DEFAULT_VERSION = Constants.Gateway.API_VERSION_2_TOKEN;
  private static final int DEFAULT_PORT = CONF.getInt(Constants.Router.ROUTER_PORT);
  private static final int DEFAULT_SSL_PORT = CONF.getInt(Constants.Router.ROUTER_SSL_PORT);
  private static final boolean DEFAULT_SSL_ENABLED = CONF.getBoolean(Constants.Security.SSL_ENABLED);
  private static final String DEFAULT_HOST = CONF.get(Constants.Router.ADDRESS);

  private boolean sslEnabled;
  private String hostname;
  private int port;
  private Supplier<AccessToken> accessToken;
  private String apiVersion;
  private String namespace;

  public ConnectionConfig(boolean sslEnabled, String hostname, int port, String namespace,
                          Supplier<AccessToken> accessToken, String apiVersion) {
    this.sslEnabled = sslEnabled;
    this.hostname = hostname;
    this.port = port;
    this.namespace = namespace;
    this.accessToken = accessToken;
    this.apiVersion = apiVersion;
  }

  public boolean isSSLEnabled() {
    return sslEnabled;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public Supplier<AccessToken> getAccessToken() {
    return accessToken;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setSslEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setAccessToken(Supplier<AccessToken> accessToken) {
    this.accessToken = accessToken;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public URI getBaseURI() {
    return URI.create(String.format("%s://%s:%d", sslEnabled ? "https" : "http", hostname, port));
  }

  /**
   * Builder for {@link ConnectionConfig}.
   */
  public static final class Builder {
    private String hostname = DEFAULT_HOST;
    private Optional<Integer> port = Optional.absent();
    private boolean sslEnabled = DEFAULT_SSL_ENABLED;
    private String apiVersion = DEFAULT_VERSION;
    private Supplier<AccessToken> accessToken = Suppliers.ofInstance(null);
    private String namespace = Constants.DEFAULT_NAMESPACE;

    public Builder setHostname(String hostname) {
      Preconditions.checkArgument(hostname != null, "hostname cannot be null");
      this.hostname = hostname;
      return this;
    }

    public Builder setPort(int port) {
      this.port = Optional.of(port);
      return this;
    }

    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setAccessToken(AccessToken accessToken) {
      this.accessToken = Suppliers.ofInstance(accessToken);
      return this;
    }

    public Builder setAccessToken(Supplier<AccessToken> accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    // TODO: handle namespace
    public Builder setUri(URI uri) {
      this.hostname = uri.getHost();
      this.sslEnabled = "https".equals(uri.getScheme());
      if (uri.getPort() != -1) {
        this.port = Optional.of(uri.getPort());
      }
      return this;
    }

    public Builder setApiVersion(String apiVersion) {
      this.apiVersion = apiVersion;
      return this;
    }

    public Builder setSSLEnabled(boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      return this;
    }

    public Builder setPort(Optional<Integer> port) {
      this.port = port;
      return this;
    }

    public ConnectionConfig build() {
      return new ConnectionConfig(sslEnabled, hostname, port.or(sslEnabled ? DEFAULT_SSL_PORT : DEFAULT_PORT),
                                  namespace, accessToken, apiVersion);
    }
  }
}