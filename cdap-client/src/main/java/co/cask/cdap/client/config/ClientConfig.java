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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.common.http.HttpRequestConfig;
import com.google.common.base.Supplier;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import javax.annotation.Nullable;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final int DEFAULT_UPLOAD_READ_TIMEOUT = 15000;
  private static final int DEFAULT_UPLOAD_CONNECT_TIMEOUT = 15000;
  private static final int DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT = 50;

  private static final int DEFAULT_READ_TIMEOUT = 15000;
  private static final int DEFAULT_CONNECT_TIMEOUT = 15000;
  private static final boolean DEFAULT_VERIFY_SSL_CERTIFICATE = true;

  private ConnectionConfig connectionConfig;

  private HttpRequestConfig defaultHttpConfig;
  private HttpRequestConfig uploadHttpConfig;

  private int unavailableRetryLimit;
  private boolean verifySSLCert;

  private ClientConfig(ConnectionConfig connectionConfig, int unavailableRetryLimit, boolean verifySSLCert,
                       HttpRequestConfig defaultHttpConfig, HttpRequestConfig uploadHttpConfig) {
    this.connectionConfig = connectionConfig;
    this.unavailableRetryLimit = unavailableRetryLimit;
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
    return resolveURL(connectionConfig.getApiVersion(), path);
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

  private URL resolveNamespacedURL(String apiVersion, String namespace, String path) throws MalformedURLException {
    return getBaseURI().resolve("/" + apiVersion + "/namespaces/" + namespace + "/" + path).toURL();
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
    return resolveNamespacedURL(Constants.Gateway.API_VERSION_3_TOKEN, connectionConfig.getNamespace(), path);
  }

  /**
   * @return the base URI of the target CDAP instance
   */
  public URI getBaseURI() {
    return connectionConfig.getBaseURI();
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

  public ConnectionConfig getConnectionConfig() {
    return connectionConfig;
  }

  public void setConnectionConfig(ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  /**
   * @return namespace currently active
   */
  public String getNamespace() {
    return connectionConfig.getNamespace();
  }

  public boolean isVerifySSLCert() {
    return verifySSLCert;
  }

  public int getUnavailableRetryLimit() {
    return unavailableRetryLimit;
  }

  @Nullable
  public AccessToken getAccessToken() {
    return connectionConfig.getAccessToken().get();
  }

  public Supplier<AccessToken> getAccessTokenSupplier() {
    return connectionConfig.getAccessToken();
  }

  public void set(ClientConfig clientConfig) {
    this.connectionConfig = clientConfig.connectionConfig;
    this.unavailableRetryLimit = clientConfig.unavailableRetryLimit;
    this.verifySSLCert = clientConfig.verifySSLCert;
    this.defaultHttpConfig = clientConfig.defaultHttpConfig;
    this.uploadHttpConfig = clientConfig.uploadHttpConfig;
  }

  public void setNamespace(String namespace) {
    connectionConfig.setNamespace(namespace);
  }

  /**
   * Builder for {@link ClientConfig}.
   */
  public static final class Builder {

    private ConnectionConfig connectionConfig = new ConnectionConfig.Builder().build();
    private int uploadReadTimeoutMs = DEFAULT_UPLOAD_READ_TIMEOUT;
    private int uploadConnectTimeoutMs = DEFAULT_UPLOAD_CONNECT_TIMEOUT;
    private int serviceUnavailableRetryLimit = DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT;

    private int defaultReadTimeoutMs = DEFAULT_READ_TIMEOUT;
    private int defaultConnectTimeoutMs = DEFAULT_CONNECT_TIMEOUT;
    private boolean verifySSLCert = DEFAULT_VERIFY_SSL_CERTIFICATE;

    public Builder() { }

    public Builder(ClientConfig clientConfig) {
      this.connectionConfig = clientConfig.getConnectionConfig();
      this.uploadReadTimeoutMs = clientConfig.getUploadHttpConfig().getReadTimeout();
      this.uploadConnectTimeoutMs = clientConfig.getUploadHttpConfig().getConnectTimeout();
      this.defaultReadTimeoutMs = clientConfig.getDefaultHttpConfig().getReadTimeout();
      this.defaultConnectTimeoutMs = clientConfig.getDefaultHttpConfig().getConnectTimeout();
      this.verifySSLCert = clientConfig.isVerifySSLCert();
    }

    public Builder setConnectionConfig(ConnectionConfig connectionConfig) {
      this.connectionConfig = connectionConfig;
      return this;
    }

    public Builder setUploadReadTimeoutMs(int uploadReadTimeoutMs) {
      this.uploadReadTimeoutMs = uploadReadTimeoutMs;
      return this;
    }

    public Builder setVerifySSLCert(boolean verifySSLCert) {
      this.verifySSLCert = verifySSLCert;
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

    public Builder setServiceUnavailableRetryLimit(int retry) {
      this.serviceUnavailableRetryLimit = retry;
      return this;
    }

    public ClientConfig build() {
      return new ClientConfig(connectionConfig, serviceUnavailableRetryLimit, verifySSLCert,
                              new HttpRequestConfig(defaultConnectTimeoutMs, defaultReadTimeoutMs, verifySSLCert),
                              new HttpRequestConfig(uploadConnectTimeoutMs, uploadReadTimeoutMs, verifySSLCert));
    }
  }
}
