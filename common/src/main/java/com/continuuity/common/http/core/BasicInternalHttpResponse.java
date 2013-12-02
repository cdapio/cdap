package com.continuuity.common.http.core;

import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class that stores the response body in memory or as a file.
 */
public class BasicInternalHttpResponse implements InternalHttpResponse {
  private final int statusCode;
  private final InputSupplier<? extends InputStream> inputSupplier;

  public BasicInternalHttpResponse(int statusCode, InputSupplier<? extends InputStream> inputSupplier) {
    this.statusCode = statusCode;
    this.inputSupplier = inputSupplier;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public InputSupplier<? extends InputStream> getInputSupplier() throws IOException {
    return inputSupplier;
  }
}
