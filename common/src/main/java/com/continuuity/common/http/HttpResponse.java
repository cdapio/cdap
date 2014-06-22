/*
 * Copyright Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.http;

/**
 * Return type for http requests executed by {@link HttpResponse}
 */
public class HttpResponse {
  private final int responseCode;
  private final String responseMessage;
  private final byte[] responseBody;

  HttpResponse(int responseCode, String responseMessage, byte[] responseBody) {
    this.responseCode = responseCode;
    this.responseMessage = responseMessage;
    this.responseBody = responseBody;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  public byte[] getResponseBody() {
    return responseBody;
  }
}
