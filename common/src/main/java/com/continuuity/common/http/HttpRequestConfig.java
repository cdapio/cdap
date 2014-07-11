/*
 * Copyright (c) 2012-2014 Continuuity Inc. All rights reserved.
 */
package com.continuuity.common.http;

/**
 *
 */
public class HttpRequestConfig {

  public static final HttpRequestConfig DEFAULT = new HttpRequestConfig(0, 0);

  private final int connectTimeout;
  private final int readTimeout;

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
