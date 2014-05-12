package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;

import java.io.IOException;

/**
 * Maintains secret keys in memory and uses them to sign and validate authentication tokens.
 */
public class InMemoryKeyManager extends MapBackedKeyManager {

  /**
   * Create an InMemoryKeyManager that stores keys in memory only.
   * @param conf
   */
  public InMemoryKeyManager(CConfiguration conf) {
    super(conf);
  }

  @Override
  public void doInit() throws IOException {
    generateKey();
  }

  @Override
  public void shutDown() {
    // nothing to do
  }
}
