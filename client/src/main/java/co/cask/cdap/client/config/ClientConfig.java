/*
 * Copyright 2014 Cask Data, Inc.
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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Configuration for the Java client API
 */
public class ClientConfig {

  private static final String DEFAULT_PROTOCOL = "http";
  private static final String VERSION = "v2";

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  private String protocol;
  private URI baseURI;
  private int port;
  private String accessToken;

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   * @throws URISyntaxException
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig,
                      HttpRequestConfig uploadConfig) throws URISyntaxException {
    this(hostname, port, defaultConfig, uploadConfig, DEFAULT_PROTOCOL);
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   * @param protocol http/https according to SSL is enabled in the gateway server or not
   * @throws URISyntaxException
   */
  public ClientConfig(String hostname, int port, HttpRequestConfig defaultConfig,
                      HttpRequestConfig uploadConfig, String protocol) throws URISyntaxException {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
    this.port = port;
    this.protocol = protocol;
    this.baseURI = new URI(String.format("%s://%s:%d", protocol, hostname, port));
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @throws URISyntaxException
   */
  public ClientConfig(String hostname, int port) throws URISyntaxException {
    this(hostname, port, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0));
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @throws URISyntaxException
   */
  public ClientConfig(String hostname) throws URISyntaxException {
    this(hostname, 10000, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0));
  }

  /**
   * Resolves a path against the target CDAP server
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v2/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveURL(String path) throws MalformedURLException {
    return baseURI.resolve("/" + VERSION + "/" + path).toURL();
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
   * @return port of Reactor
   */
  public int getPort() {
    return port;
  }

  /**
   * @param hostname Hostname of the CDAP server (i.e. example.com)
   * @param port Port of the CDAP server (i.e. 10000)
   * @throws URISyntaxException
   */
  public void setHostnameAndPort(String hostname, int port) throws URISyntaxException {
    this.port = port;
    this.baseURI = new URI(protocol + "://" + hostname + ":" + port);
  }

  /**
   * @param accessToken the accessToken to set
   */
  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  /**
   * @return the accessToken
   */
  public String getAccessToken() {
    return accessToken;
  }

  /**
   * @return the protocol
   */
  public String getProtocol() {
    return protocol;
  }

  /**
   * @param protocol the protocol to set
   */
  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }
}
