/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.common.http.HttpRequestConfig;
import co.cask.cdap.security.authentication.client.AccessToken;
import co.cask.cdap.security.authentication.client.AuthenticationClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final boolean DEFAULT_IS_SSL_ENABLED = false;
  private static final String DEFAULT_VERSION = "v2";
  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final int DEFAULT_PORT = 10000;

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  private String protocol;
  private URI baseURI;
  private int port;
  private AuthenticationClient authenticationClient;

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig,
                      AuthenticationClient authenticationClient) {
    this(hostname, port, defaultConfig, uploadConfig, DEFAULT_IS_SSL_ENABLED, authenticationClient);
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   * @param isSslEnabled true, if SSL is enabled in the gateway server
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig,
                      boolean isSslEnabled, AuthenticationClient authenticationClient) {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
    this.port = port;
    this.protocol = isSslEnabled ? HTTPS : HTTP;
    this.authenticationClient = authenticationClient;
    this.baseURI = URI.create(String.format("%s://%s:%d", protocol, hostname, port));
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   */
  public ClientConfig(String hostname, int port, AuthenticationClient authenticationClient) {
    this(hostname, port, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0), authenticationClient);
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   */
  public ClientConfig(String hostname, AuthenticationClient authenticationClient) {
    this(hostname, DEFAULT_PORT, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0),
         authenticationClient);
  }

  /**
   * Resolves a path against the target CDAP server
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v2/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveURL(String path) throws MalformedURLException {
    return baseURI.resolve("/" + DEFAULT_VERSION + "/" + path).toURL();
  }

  /**
   * @return {@link HttpRequestConfig} to use by default
   */
  public HttpRequestConfig getDefaultConfig() {
    return defaultConfig;
  }

  /**
   * @return {@link HttpRequestConfig} to use when uploading a file
   */
  public HttpRequestConfig getUploadConfig() {
    return uploadConfig;
  }

  /**
   * @return port of CDAP
   */
  public int getPort() {
    return port;
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   */
  public void setHostnameAndPort(String hostname, int port, boolean isSslEnabled) {
    this.port = port;
    this.protocol = isSslEnabled ? HTTPS : HTTP;
    this.baseURI = URI.create(String.format("%s://%s:%d", protocol, hostname, port));
  }

  /**
   * @return the accessToken
   */
  public AccessToken getAccessToken() throws IOException {
    return (authenticationClient.isAuthEnabled()) ? authenticationClient.getAccessToken() : null;
  }

  /**
   * @param authenticationClient the authenticationClient to set
   */
  public void setAuthenticationClient(AuthenticationClient authenticationClient) {
    this.authenticationClient = authenticationClient;
  }

  public AuthenticationClient getAuthenticationClient() {
    return authenticationClient;
  }

  /**
   * @param protocol the protocol to set
   */
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }
}
