package com.continuuity.hive.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Implementation of hive client used when explore functionality is disabled.
 */
public class DisabledHiveClient implements HiveClient {

  @Override
  public void sendCommand(String command, OutputStream out, OutputStream err) throws IOException {
    throw new UnsupportedOperationException("Explore module is disabled.");
  }
}
