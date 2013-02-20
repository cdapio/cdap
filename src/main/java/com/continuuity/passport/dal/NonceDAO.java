package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.StaleNonceException;

/**
 *
 */
public interface NonceDAO {

  public enum NONCE_TYPE {
    SESSION,
    ACTIVATION
  }

  public int getNonce(int id, NONCE_TYPE type) throws RuntimeException;

  public int getId(int nonce, NONCE_TYPE type) throws RuntimeException, StaleNonceException;
}
