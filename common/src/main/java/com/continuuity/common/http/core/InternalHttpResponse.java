package com.continuuity.common.http.core;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface used to get the status code and content from calling another handler internally.
 */
public interface InternalHttpResponse {

  public int getStatusCode();

  public InputStream getInputStream() throws IOException;
}
