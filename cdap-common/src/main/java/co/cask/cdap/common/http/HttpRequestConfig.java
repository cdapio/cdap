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
package co.cask.cdap.common.http;

import java.net.HttpURLConnection;

/**
 * Configuration per HTTP request executed by {@link HttpRequests}.
 */
public class HttpRequestConfig {

  public static final HttpRequestConfig DEFAULT = new HttpRequestConfig(15000, 15000);
  private static final int DEFAULT_FIXED_LENGTH_STREAMING_THRESHOLD = 256 * 1024; // 256K

  private final int connectTimeout;
  private final int readTimeout;
  private final boolean verifySSLCert;
  private final int fixedLengthStreamingThreshold;

  /**
   * @param connectTimeout Connect timeout, in milliseconds. See {@link java.net.URLConnection#getConnectTimeout()}.
   * @param readTimeout Read timeout, in milliseconds. See {@link java.net.URLConnection#getReadTimeout()}.
   */
  public HttpRequestConfig(int connectTimeout, int readTimeout) {
    this(connectTimeout, readTimeout, true);
  }

  /**
   * @param connectTimeout Connect timeout, in milliseconds. See {@link java.net.URLConnection#getConnectTimeout()}.
   * @param readTimeout Read timeout, in milliseconds. See {@link java.net.URLConnection#getReadTimeout()}.
   * @param verifySSLCert false, to disable certificate verifying in SSL connections. By default SSL certificate is
   *                      verified.
   */
  public HttpRequestConfig(int connectTimeout, int readTimeout, boolean verifySSLCert) {
    this(connectTimeout, readTimeout, verifySSLCert, DEFAULT_FIXED_LENGTH_STREAMING_THRESHOLD);
  }

  /**
   * @param connectTimeout Connect timeout, in milliseconds. See {@link java.net.URLConnection#getConnectTimeout()}.
   * @param readTimeout Read timeout, in milliseconds. See {@link java.net.URLConnection#getReadTimeout()}.
   * @param verifySSLCert false, to disable certificate verifying in SSL connections. By default SSL certificate is
   *                      verified.
   * @param fixedLengthStreamingThreshold number of bytes in the request body to use fix length request mode. See
   *                                  {@link HttpURLConnection#setFixedLengthStreamingMode(int)}.
   */
  public HttpRequestConfig(int connectTimeout, int readTimeout,
                           boolean verifySSLCert, int fixedLengthStreamingThreshold) {
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
    this.verifySSLCert = verifySSLCert;
    this.fixedLengthStreamingThreshold = fixedLengthStreamingThreshold;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getReadTimeout() {
    return readTimeout;
  }

  public boolean isVerifySSLCert() {
    return verifySSLCert;
  }

  public int getFixedLengthStreamingThreshold() {
    return fixedLengthStreamingThreshold;
  }
}
