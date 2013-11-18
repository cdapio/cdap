package com.continuuity.common.http.core;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class that stores the response body in memory or as a file.
 */
public class BasicInternalHttpResponse implements InternalHttpResponse {
  private final int statusCode;
  private final byte[] body;
  private final File file;

  public BasicInternalHttpResponse(int statusCode, byte[] body, File file) {
    this.statusCode = statusCode;
    this.body = body;
    this.file = file;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public InputStream getInputStream() throws IOException {
    if (file != null) {
      return new FileInputStream(file);
    } else {
      return new ByteArrayInputStream(body);
    }
  }
}
