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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final HttpRequestConfig DEFAULT_UPLOAD_CONFIG = new HttpRequestConfig(0, 0);
  private static final HttpRequestConfig DEFAULT_CONFIG = new HttpRequestConfig(15000, 15000);

  private static final boolean DEFAULT_IS_SSL_ENABLED = false;
  private static final String DEFAULT_VERSION = "v2";
  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  private static final int DEFAULT_PORT = 10000;

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  private String protocol;
  private URI baseURI;
  private String hostname;
  private int port;
  private AccessToken accessToken;

  /**
   * @param hostname Hostname of the CDAP instance (e.g. example.com)
   * @param port Port of the CDAP server (e.g. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig,
                      AccessToken accessToken) {
    this(hostname, port, defaultConfig, uploadConfig, DEFAULT_IS_SSL_ENABLED, accessToken);
  }

  /**
   * @param hostname hostname of the CDAP instance (e.g. example.com)
   * @param port port of the CDAP server (e.g. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   * @param isSslEnabled true, if SSL is enabled in the gateway server
   * @param accessToken access token to use when executing requests
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig, HttpRequestConfig uploadConfig,
                      boolean isSslEnabled, AccessToken accessToken) {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
    this.hostname = hostname;
    this.port = port;
    this.protocol = isSslEnabled ? HTTPS : HTTP;
    this.accessToken = accessToken;
    this.baseURI = URI.create(String.format("%s://%s:%d", protocol, hostname, port));
  }

  /**
   * @param uri URI of the CDAP instance (e.g. http://example.com:10000)
   * @param accessToken the authenticationClient to set
   */
  public ClientConfig(URI uri, AccessToken accessToken) {
    this(uri.getHost(), uri.getPort(), DEFAULT_CONFIG, DEFAULT_UPLOAD_CONFIG,
         "https".equals(uri.getScheme()), accessToken);
  }

  /**
   * @param hostname Hostname of the CDAP instance (i.e. example.com)
   * @param port Port of the CDAP server (e.g. 10000)
   */
  public ClientConfig(String hostname, int port, AccessToken accessToken) {
    this(hostname, port, DEFAULT_CONFIG, DEFAULT_UPLOAD_CONFIG, accessToken);
  }

  /**
   * @param hostname Hostname of the CDAP instance (e.g. example.com)
   */
  public ClientConfig(String hostname, AccessToken accessToken) {
    this(hostname, DEFAULT_PORT, DEFAULT_CONFIG, DEFAULT_UPLOAD_CONFIG, accessToken);
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
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   */
  public void setHostnameAndPort(String hostname, int port, boolean isSslEnabled) {
    this.hostname = hostname;
    this.port = port;
    this.protocol = isSslEnabled ? HTTPS : HTTP;
    this.baseURI = URI.create(String.format("%s://%s:%d", protocol, hostname, port));
  }

  /**
   * @return the base URI of the target CDAP instance
   */
  public URI getBaseURI() {
    return baseURI;
  }

  /**
   * @return the accessToken
   */
  public AccessToken getAccessToken() {
    return accessToken;
  }

  /**
   * @param accessToken the access token to set
   */
  public void setAccessToken(AccessToken accessToken) {
    this.accessToken = accessToken;
  }

  /**
   * @param protocol the protocol to set
   */
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }
}
