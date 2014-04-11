package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import java.security.NoSuchAlgorithmException;

/**
 * Maintains secret keys in memory and uses them to sign and validate authentication tokens.
 */
public class InMemoryKeyManager extends AbstractKeyManager {

  public InMemoryKeyManager() {
    super();
  }

  public InMemoryKeyManager(CConfiguration conf) {
    super(conf);
  }

  public InMemoryKeyManager(String keyAlgo, int keyLength) {
    super(keyAlgo, keyLength);
  }

  public void init() throws NoSuchAlgorithmException {
    keyGenerator = KeyGenerator.getInstance(keyAlgo);
    keyGenerator.init(keyLength);

    threadLocalMac = new ThreadLocal<Mac>() {
      @Override
      public Mac initialValue() {
        try {
          return Mac.getInstance(keyAlgo);
        } catch (NoSuchAlgorithmException nsae) {
          throw new IllegalArgumentException("Unknown algorithm for secret keys: " + keyAlgo);
        }
      }
    };
    generateKey();
  }
}
