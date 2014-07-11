/*
 * Copyright (c) 2012-2014 Continuuity Inc. All rights reserved.
 */
package com.continuuity.common.http;

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
