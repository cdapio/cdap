/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.client.config;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.client.exception.DisconnectedException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authentication.client.AccessToken;
import io.cdap.common.http.HttpRequestConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final boolean DEFAULT_VERIFY_SSL_CERTIFICATE = true;

  private static final int DEFAULT_UPLOAD_READ_TIMEOUT = 0;
  private static final int DEFAULT_UPLOAD_CONNECT_TIMEOUT = 0;
  private static final int DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT = 50;

  private static final int DEFAULT_READ_TIMEOUT = 15000;
  private static final int DEFAULT_CONNECT_TIMEOUT = 15000;

  private static final String DEFAULT_VERSION = Constants.Gateway.API_VERSION_3_TOKEN;

  @Nullable
  private ConnectionConfig connectionConfig;
  private boolean verifySSLCert;

  private int defaultReadTimeout;
  private int defaultConnectTimeout;
  private int uploadReadTimeout;
  private int uploadConnectTimeout;

  private int unavailableRetryLimit;
  private String apiVersion;
  private Supplier<AccessToken> accessToken;
  private Map<String, String> additionalHeaders;

  private ClientConfig(@Nullable ConnectionConfig connectionConfig,
                       boolean verifySSLCert, int unavailableRetryLimit,
                       String apiVersion, Supplier<AccessToken> accessToken,
                       int defaultReadTimeout, int defaultConnectTimeout,
                       int uploadReadTimeout, int uploadConnectTimeout,
                       Map<String, String> additionalHeaders) {
    this.connectionConfig = connectionConfig;
    this.verifySSLCert = verifySSLCert;
    this.apiVersion = apiVersion;
    this.unavailableRetryLimit = unavailableRetryLimit;
    this.accessToken = accessToken;
    this.defaultReadTimeout = defaultReadTimeout;
    this.defaultConnectTimeout = defaultConnectTimeout;
    this.uploadReadTimeout = uploadReadTimeout;
    this.uploadConnectTimeout = uploadConnectTimeout;
    this.additionalHeaders = additionalHeaders;
  }

  public static ClientConfig getDefault() {
    return ClientConfig.builder().build();
  }

  private URL resolveURL(String apiVersion, String path) throws DisconnectedException, MalformedURLException {
    return getConnectionConfig().resolveURI(apiVersion, path).toURL();
  }

  /**
   * Resolves a path against the target CDAP server
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:11015/v2/apps".
   * @return URL of the resolved path
   */
  public URL resolveURL(String path) throws DisconnectedException, MalformedURLException {
    return resolveURL(apiVersion, path);
  }

  public URL resolveURL(String format, Object... args) throws MalformedURLException {
    return resolveURL(apiVersion, String.format(format, args));
  }

  /**
   * Resolves a path against the target CDAP server
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:11015/v2/apps".
   * @return URL of the resolved path
   */
  public URL resolveURLV3(String path) throws MalformedURLException {
    return resolveURL(Constants.Gateway.API_VERSION_3_TOKEN, path);
  }

  /**
   * Resolves a path aginst the CDAP server, without applying an API version. For example, /ping
   */
  public URL resolveURLNoVersion(String path) throws MalformedURLException {
    return getConnectionConfig().resolveURI(path).toURL();
  }

  /**
   * Resolves a path against the target CDAP server with the provided namespace, using V3 APIs
   *
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:11015/v3/&lt;namespace&gt;/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveNamespacedURLV3(NamespaceId namespace, String path) throws MalformedURLException {
    return getConnectionConfig().resolveNamespacedURI(namespace, Constants.Gateway.API_VERSION_3_TOKEN, path).toURL();
  }

  public HttpRequestConfig getDefaultRequestConfig() {
    if (connectionConfig == null) {
      throw new DisconnectedException();
    }
    return new HttpRequestConfig(defaultConnectTimeout, defaultReadTimeout, verifySSLCert);
  }

  /**
   * @return a {@link HttpRequestConfig} used for operations whose duration varies based on the operation, such as
   * application deployment and query execution.
   */
  public HttpRequestConfig getUploadRequestConfig() {
    if (connectionConfig == null) {
      throw new DisconnectedException();
    }
    return new HttpRequestConfig(uploadConnectTimeout, uploadReadTimeout, verifySSLCert);
  }

  public int getDefaultReadTimeout() {
    return defaultReadTimeout;
  }

  public int getDefaultConnectTimeout() {
    return defaultConnectTimeout;
  }

  public int getUploadReadTimeout() {
    return uploadReadTimeout;
  }

  public int getUploadConnectTimeout() {
    return uploadConnectTimeout;
  }

  public Map<String, String> getAdditionalHeaders() {
    return additionalHeaders;
  }

  public void setConnectionConfig(@Nullable ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  public ConnectionConfig getConnectionConfig() {
    if (connectionConfig == null) {
      throw new DisconnectedException();
    }
    return connectionConfig;
  }

  public boolean isVerifySSLCert() {
    return verifySSLCert;
  }

  public void setVerifySSLCert(boolean verifySSLCert) {
    this.verifySSLCert = verifySSLCert;
  }

  public void setDefaultReadTimeout(int defaultReadTimeout) {
    this.defaultReadTimeout = defaultReadTimeout;
  }

  public void setDefaultConnectTimeout(int defaultConnectTimeout) {
    this.defaultConnectTimeout = defaultConnectTimeout;
  }

  public void setUploadReadTimeout(int uploadReadTimeout) {
    this.uploadReadTimeout = uploadReadTimeout;
  }

  public void setUploadConnectTimeout(int uploadConnectTimeout) {
    this.uploadConnectTimeout = uploadConnectTimeout;
  }

  public void setUnavailableRetryLimit(int unavailableRetryLimit) {
    this.unavailableRetryLimit = unavailableRetryLimit;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public int getUnavailableRetryLimit() {
    return unavailableRetryLimit;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
  }

  public void setAllTimeouts(int timeout) {
    this.defaultConnectTimeout = timeout;
    this.defaultReadTimeout = timeout;
    this.uploadConnectTimeout = timeout;
    this.uploadReadTimeout = timeout;
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

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link ClientConfig}.
   */
  public static final class Builder {

    private ConnectionConfig connectionConfig = ConnectionConfig.DEFAULT;
    private String apiVersion = DEFAULT_VERSION;
    private Supplier<AccessToken> accessToken = Suppliers.ofInstance(null);
    private boolean verifySSLCert = DEFAULT_VERIFY_SSL_CERTIFICATE;

    private int uploadReadTimeout = DEFAULT_UPLOAD_READ_TIMEOUT;
    private int uploadConnectTimeout = DEFAULT_UPLOAD_CONNECT_TIMEOUT;
    private int defaultReadTimeout = DEFAULT_READ_TIMEOUT;
    private int defaultConnectTimeout = DEFAULT_CONNECT_TIMEOUT;

    private int unavailableRetryLimit = DEFAULT_SERVICE_UNAVAILABLE_RETRY_LIMIT;
    private Map<String, String> additionalHeaders = new HashMap<>();

    public Builder() { }

    public Builder(ClientConfig clientConfig) {
      this.connectionConfig = clientConfig.connectionConfig;
      this.verifySSLCert = clientConfig.verifySSLCert;
      this.apiVersion = clientConfig.apiVersion;
      this.accessToken = clientConfig.accessToken;
      this.uploadReadTimeout = clientConfig.uploadReadTimeout;
      this.uploadConnectTimeout = clientConfig.uploadConnectTimeout;
      this.defaultReadTimeout = clientConfig.defaultReadTimeout;
      this.defaultConnectTimeout = clientConfig.defaultConnectTimeout;
      this.unavailableRetryLimit = clientConfig.unavailableRetryLimit;
    }

    public Builder setConnectionConfig(ConnectionConfig connectionConfig) {
      this.connectionConfig = connectionConfig;
      return this;
    }

    public Builder setVerifySSLCert(boolean verifySSLCert) {
      this.verifySSLCert = verifySSLCert;
      return this;
    }

    public Builder setUploadReadTimeout(int uploadReadTimeout) {
      this.uploadReadTimeout = uploadReadTimeout;
      return this;
    }

    public Builder setUploadConnectTimeout(int uploadConnectTimeout) {
      this.uploadConnectTimeout = uploadConnectTimeout;
      return this;
    }

    public Builder setDefaultReadTimeout(int defaultReadTimeout) {
      this.defaultReadTimeout = defaultReadTimeout;
      return this;
    }

    public Builder setDefaultConnectTimeout(int defaultConnectTimeout) {
      this.defaultConnectTimeout = defaultConnectTimeout;
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

    public Builder setUnavailableRetryLimit(int retry) {
      this.unavailableRetryLimit = retry;
      return this;
    }

    public Builder addAdditionalHeader(String header, String value) {
      this.additionalHeaders.put(header, value);
      return this;
    }

    public ClientConfig build() {
      return new ClientConfig(connectionConfig, verifySSLCert,
                              unavailableRetryLimit, apiVersion, accessToken,
                              defaultReadTimeout, defaultConnectTimeout,
                              uploadReadTimeout, uploadConnectTimeout, ImmutableMap.copyOf(additionalHeaders));
    }
  }

}
