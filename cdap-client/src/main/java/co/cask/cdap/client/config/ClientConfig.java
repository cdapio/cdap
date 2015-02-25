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
import co.cask.cdap.proto.Id;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final CConfiguration CONF = CConfiguration.create();

  private static final int DEFAULT_UPLOAD_READ_TIMEOUT = 15000;
  private static final int DEFAULT_UPLOAD_CONNECT_TIMEOUT = 15000;
  private static final int DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT = 50;

  private static final int DEFAULT_READ_TIMEOUT = 15000;
  private static final int DEFAULT_CONNECT_TIMEOUT = 15000;
  private static final boolean DEFAULT_VERIFY_SSL_CERTIFICATE = true;

  private static final String DEFAULT_VERSION = Constants.Gateway.API_VERSION_2_TOKEN;
  private static final int DEFAULT_PORT = CONF.getInt(Constants.Router.ROUTER_PORT);
  private static final int DEFAULT_SSL_PORT = CONF.getInt(Constants.Router.ROUTER_SSL_PORT);
  private static final boolean DEFAULT_SSL_ENABLED = CONF.getBoolean(Constants.Security.SSL_ENABLED);
  private static final String DEFAULT_HOST = CONF.get(Constants.Router.ADDRESS);

  private HttpRequestConfig defaultHttpConfig;
  private HttpRequestConfig uploadHttpConfig;

  private boolean sslEnabled;
  private String hostname;
  private int port;
  private Id.Namespace namespace;
  private int unavailableRetryLimit;
  private String apiVersion;
  private Supplier<AccessToken> accessToken;
  private boolean verifySSLCert;

  private ClientConfig(String hostname, int port, Id.Namespace namespace, boolean sslEnabled, int unavailableRetryLimit,
                       String apiVersion, Supplier<AccessToken> accessToken, boolean verifySSLCert,
                       HttpRequestConfig defaultHttpConfig, HttpRequestConfig uploadHttpConfig) {
    this.hostname = hostname;
    this.apiVersion = apiVersion;
    this.port = port;
    this.namespace = namespace;
    this.sslEnabled = sslEnabled;
    this.unavailableRetryLimit = unavailableRetryLimit;
    this.accessToken = accessToken;
    this.verifySSLCert = verifySSLCert;
    this.defaultHttpConfig = defaultHttpConfig;
    this.uploadHttpConfig = uploadHttpConfig;
  }

  private URL resolveURL(String apiVersion, String path) throws MalformedURLException {
    return getBaseURI().resolve("/" + apiVersion + "/" + path).toURL();
  }

  /**
   * Resolves a path against the target CDAP server
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v2/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveURL(String path) throws MalformedURLException {
    return resolveURL(apiVersion, path);
  }

  /**
   * Resolves a path against the target CDAP server
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v2/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveURLV3(String path) throws MalformedURLException {
    return resolveURL(Constants.Gateway.API_VERSION_3_TOKEN, path);
  }

  private URL resolveNamespacedURL(String apiVersion, Id.Namespace namespace, String path) throws MalformedURLException {
    return getBaseURI().resolve("/" + apiVersion + "/namespaces/" + namespace.getId() + "/" + path).toURL();
  }

  /**
   * Resolves a path against the target CDAP server with the provided namespace, using V3 APIs
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v3/<namespace>/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveNamespacedURLV3(String path) throws MalformedURLException {
    return resolveNamespacedURL(Constants.Gateway.API_VERSION_3_TOKEN, namespace, path);
  }

  /**
   * @return the base URI of the target CDAP instance
   */
  public URI getBaseURI() {
    return URI.create(String.format("%s://%s:%d", sslEnabled ? "https" : "http", hostname, port));
  }

  /**
   * @return {@link HttpRequestConfig} to use by default
   */
  public HttpRequestConfig getDefaultHttpConfig() {
    return defaultHttpConfig;
  }

  /**
   * @return {@link HttpRequestConfig} to use when uploading a file
   */
  public HttpRequestConfig getUploadHttpConfig() {
    return uploadHttpConfig;
  }

  /**
   * @return hostname of the target CDAP instance
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @return port of the target CDAP instance
   */
  public int getPort() {
    return port;
  }

  /**
   * @return namespace currently active
   */
  public Id.Namespace getNamespace() {
    return namespace;
  }

  public boolean isVerifySSLCert() {
    return verifySSLCert;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public boolean isSSLEnabled() {
    return sslEnabled;
  }

  public int getUnavailableRetryLimit() {
    return unavailableRetryLimit;
  }

  public void setSSLEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
  }

  public void setHostname(String hostname) {
    Preconditions.checkArgument(hostname != null, "hostname cannot be null");
    this.hostname = hostname;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setNamespace(Id.Namespace namespace) {
    this.namespace = namespace;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
  }

  public void setVerifySSLCert(boolean verifySSLCert) {
    this.verifySSLCert = verifySSLCert;
    this.defaultHttpConfig = new HttpRequestConfig(defaultHttpConfig.getConnectTimeout(),
                                                   defaultHttpConfig.getReadTimeout(), verifySSLCert);
    this.uploadHttpConfig = new HttpRequestConfig(uploadHttpConfig.getConnectTimeout(),
                                                  uploadHttpConfig.getReadTimeout(), verifySSLCert);
  }

  public void setAllTimeouts(int timeout) {
    this.defaultHttpConfig = new HttpRequestConfig(timeout, timeout, verifySSLCert);
    this.uploadHttpConfig = new HttpRequestConfig(timeout, timeout, verifySSLCert);
  }

  public void setURI(URI uri) {
    this.hostname = uri.getHost();
    this.sslEnabled = "https".equals(uri.getScheme());
    if (uri.getPort() != -1) {
      this.port = uri.getPort();
    }
  }

  @Nullable
  public AccessToken getAccessToken() {
    return accessToken.get();
  }

  public Supplier<AccessToken> getAccessTokenSupplier() {
    return accessToken;
  }

  public void setAccessToken(Supplier<AccessToken> accessToken) {
    this.accessToken = accessToken;
  }

  public void setAccessToken(AccessToken accessToken) {
    this.accessToken = Suppliers.ofInstance(accessToken);
  }

  /**
   * Builder for {@link ClientConfig}.
   */
  public static final class Builder {

    private String hostname = DEFAULT_HOST;
    private Optional<Integer> port = Optional.absent();
    private Id.Namespace namespace = Id.Namespace.from(Constants.DEFAULT_NAMESPACE);
    private boolean sslEnabled = DEFAULT_SSL_ENABLED;
    private String apiVersion = DEFAULT_VERSION;
    private Supplier<AccessToken> accessToken = Suppliers.ofInstance(null);

    private int uploadReadTimeoutMs = DEFAULT_UPLOAD_READ_TIMEOUT;
    private int uploadConnectTimeoutMs = DEFAULT_UPLOAD_CONNECT_TIMEOUT;
    private int serviceUnavailableRetryLimit = DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT;

    private int defaultReadTimeoutMs = DEFAULT_READ_TIMEOUT;
    private int defaultConnectTimeoutMs = DEFAULT_CONNECT_TIMEOUT;
    private boolean verifySSLCert = DEFAULT_VERIFY_SSL_CERTIFICATE;

    public Builder() { }

    public Builder(ClientConfig clientConfig) {
      this.hostname = clientConfig.getHostname();
      this.port = Optional.of(clientConfig.getPort());
      this.namespace = clientConfig.getNamespace();
      this.sslEnabled = clientConfig.isSSLEnabled();
      this.apiVersion = clientConfig.getApiVersion();
      this.accessToken = clientConfig.getAccessTokenSupplier();
      this.uploadReadTimeoutMs = clientConfig.getUploadHttpConfig().getReadTimeout();
      this.uploadConnectTimeoutMs = clientConfig.getUploadHttpConfig().getConnectTimeout();
      this.defaultReadTimeoutMs = clientConfig.getDefaultHttpConfig().getReadTimeout();
      this.defaultConnectTimeoutMs = clientConfig.getDefaultHttpConfig().getConnectTimeout();
      this.verifySSLCert = clientConfig.isVerifySSLCert();
    }

    public Builder setUploadReadTimeoutMs(int uploadReadTimeoutMs) {
      this.uploadReadTimeoutMs = uploadReadTimeoutMs;
      return this;
    }

    public Builder setUploadConnectTimeoutMs(int uploadConnectTimeoutMs) {
      this.uploadConnectTimeoutMs = uploadConnectTimeoutMs;
      return this;
    }

    public Builder setDefaultReadTimeoutMs(int defaultReadTimeoutMs) {
      this.defaultReadTimeoutMs = defaultReadTimeoutMs;
      return this;
    }

    public Builder setDefaultConnectTimeoutMs(int defaultConnectTimeoutMs) {
      this.defaultConnectTimeoutMs = defaultConnectTimeoutMs;
      return this;
    }

    public Builder setVerifySSLCert(boolean verifySSLCert) {
      this.verifySSLCert = verifySSLCert;
      return this;
    }

    public Builder setSSLEnabled(boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      return this;
    }

    public Builder setHostname(String hostname) {
      Preconditions.checkArgument(hostname != null, "hostname cannot be null");
      this.hostname = hostname;
      return this;
    }

    public Builder setPort(int port) {
      this.port = Optional.of(port);
      return this;
    }

    public Builder setNamespace(Id.Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder setAccessToken(Supplier<AccessToken> accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public Builder setAccessToken(AccessToken accessToken) {
      this.accessToken = Suppliers.ofInstance(accessToken);
      return this;
    }

    public Builder setApiVersion(String apiVersion) {
      this.apiVersion = apiVersion;
      return this;
    }

    public Builder setServiceUnavailableRetryLimit(int retry) {
      this.serviceUnavailableRetryLimit = retry;
      return this;
    }

    public Builder setUri(URI uri) {
      this.hostname = uri.getHost();
      this.sslEnabled = "https".equals(uri.getScheme());
      if (uri.getPort() != -1) {
        this.port = Optional.of(uri.getPort());
      }
      return this;
    }

    public ClientConfig build() {
      return new ClientConfig(hostname, port.or(sslEnabled ? DEFAULT_SSL_PORT : DEFAULT_PORT), namespace,
                              sslEnabled, serviceUnavailableRetryLimit, apiVersion, accessToken, verifySSLCert,
                              new HttpRequestConfig(defaultConnectTimeoutMs, defaultReadTimeoutMs, verifySSLCert),
                              new HttpRequestConfig(uploadConnectTimeoutMs, uploadReadTimeoutMs, verifySSLCert));
    }
  }
}
