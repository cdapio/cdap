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
package co.cask.cdap.common.http;

/**
 * Configuration per HTTP request executed by {@link HttpRequests}.
 */
public class HttpRequestConfig {

  public static final HttpRequestConfig DEFAULT = new HttpRequestConfig(0, 0);

  private final int connectTimeout;
  private final int readTimeout;

  /**
   * @param connectTimeout Connect timeout, in milliseconds. See {@link java.net.URLConnection#getConnectTimeout()}.
   * @param readTimeout Read timeout, in milliseconds. See {@link java.net.URLConnection#getReadTimeout()}.
   */
  public HttpRequestConfig(int connectTimeout, int readTimeout) {
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }
}
