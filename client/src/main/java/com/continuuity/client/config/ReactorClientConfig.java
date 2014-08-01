/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.client.config;

import com.continuuity.common.http.HttpRequestConfig;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Configuration for the Reactor Java client library
 */
public class ReactorClientConfig {

  private String protocol = "http";
  private String version = "v2";

  private final HttpRequestConfig defaultConfig;
  private final HttpRequestConfig uploadConfig;

  private URI baseURI;
  private int port;

  /**
   * @param reactorHost Hostname of Reactor (i.e. example.com)
   * @param port Port of Reactor (i.e. 10000)
   * @param defaultConfig {@link HttpRequestConfig} to use by default
   * @param uploadConfig {@link HttpRequestConfig} to use when uploading a file
   * @throws URISyntaxException
   */
  public ReactorClientConfig(String reactorHost, int port, HttpRequestConfig defaultConfig,
                             HttpRequestConfig uploadConfig) throws URISyntaxException {
    this.defaultConfig = defaultConfig;
    this.uploadConfig = uploadConfig;
    this.port = port;
    this.baseURI = new URI(protocol + "://" + reactorHost + ":" + port);
  }

  /**
   * @param reactorHost Hostname of Reactor (i.e. example.com)
   * @param port Port of Reactor (i.e. 10000)
   * @throws URISyntaxException
   */
  public ReactorClientConfig(String reactorHost, int port) throws URISyntaxException {
    this(reactorHost, port, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0));
  }

  /**
   * @param reactorHost Hostname of Reactor (i.e. example.com)
   * @throws URISyntaxException
   */
  public ReactorClientConfig(String reactorHost) throws URISyntaxException {
    this(reactorHost, 10000, new HttpRequestConfig(15000, 15000), new HttpRequestConfig(0, 0));
  }

  /**
   * Resolves a path against the target Reactor
   * @param path Path to the HTTP endpoint. For example, "apps" would result
   *             in a URL like "http://example.com:10000/v2/apps".
   * @return URL of the resolved path
   * @throws MalformedURLException
   */
  public URL resolveURL(String path) throws MalformedURLException {
    return baseURI.resolve("/" + version + "/" + path).toURL();
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
   * @param reactorHost Hostname of Reactor (i.e. example.com)
   * @param port Port of Reactor (i.e. 10000)
   * @throws URISyntaxException
   */
  public void setReactorHost(String reactorHost, int port) throws URISyntaxException {
    this.port = port;
    this.baseURI = new URI(protocol + "://" + reactorHost + ":" + port);
  }
}
