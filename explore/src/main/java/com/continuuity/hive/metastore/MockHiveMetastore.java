package com.continuuity.hive.metastore;

/**
 * Mock implementation of hive metastore - it does nothing.
 */
public class MockHiveMetastore extends HiveMetastore {

  @Override
  protected void startUp() throws Exception {
    // do nothing
  }

  @Override
  protected void shutDown() throws Exception {
    // do nothing
  }
}
