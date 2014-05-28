package com.continuuity.hive.client;

import java.io.IOException;

/**
 * No operation hive client implementation.
 */
public class NoOpHiveClient implements HiveClient {

  @Override
  public void sendCommand(String command) throws IOException {
    // do nothing
  }
}
