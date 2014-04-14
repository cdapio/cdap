package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Maintains secret keys in memory and uses them to sign and validate authentication tokens.
 */
public class InMemoryKeyManager extends AbstractKeyManager {

  /**
   * Create an InMemoryKeyManager that stores keys in memory only.
   * @param conf
   */
  public InMemoryKeyManager(CConfiguration conf) {
    super(conf);
  }

  @Override
  public void init() throws NoSuchAlgorithmException, IOException {
    super.init();
    generateKey();
  }
}
