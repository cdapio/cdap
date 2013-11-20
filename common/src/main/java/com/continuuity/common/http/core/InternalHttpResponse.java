package com.continuuity.common.http.core;

import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;

/**
 * Interface used to get the status code and content from calling another handler internally.
 */
public interface InternalHttpResponse {

  int getStatusCode();

  InputSupplier<? extends InputStream> getInputSupplier() throws IOException;
}
