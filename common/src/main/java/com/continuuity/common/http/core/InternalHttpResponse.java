package com.continuuity.common.http.core;

import java.io.InputStream;

/**
 * Class used to get the status code and content from calling another handler internally.
 */
public class InternalHttpResponse {
  private final int statusCode;
  private final InputStream inputStream;

  public InternalHttpResponse(int statusCode, InputStream inputStream) {
    this.statusCode = statusCode;
    this.inputStream = inputStream;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public InputStream getInputStream() {
    return inputStream;
  }
}
