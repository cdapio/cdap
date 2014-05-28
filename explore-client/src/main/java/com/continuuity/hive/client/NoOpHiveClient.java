package com.continuuity.hive.client;

import java.io.IOException;
import java.io.OutputStream;

/**
 * No operation hive client implementation.
 */
public class NoOpHiveClient implements HiveClient {

  @Override
  public void sendCommand(String command, OutputStream out, OutputStream err) throws IOException {
    // do nothing
  }
}
