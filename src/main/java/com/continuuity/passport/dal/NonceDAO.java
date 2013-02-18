package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.StaleNonceException;

/**
 *
 */
public interface NonceDAO {

  public int getNonce(int id) throws RuntimeException;

  public int getId(int nonce) throws RuntimeException, StaleNonceException;
}
