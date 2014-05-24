package com.continuuity.hive.server;

/**
 * Mock implementation of HiveServer, in case Hive jars are not present in reactor.
 */
public class MockHiveServer extends HiveServer {

  @Override
  protected void startUp() throws Exception {
    // do nothing
  }

  @Override
  protected void shutDown() throws Exception {
    // do nothing
  }
}
