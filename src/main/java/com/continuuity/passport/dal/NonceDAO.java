/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.StaleNonceException;

/**
 *  Manage Account activation and session nonce.
 */
public interface NonceDAO {

  public enum NONCE_TYPE {
    SESSION,
    ACTIVATION
  }

  /**
   * Generates random nonce for id
   * @param id  id to be nonced
   * @param type Nonce type that determines expiration
   * @return  random nonce
   * @throws RuntimeException on errors from underlying DAO
   */
  public int getNonce(int id, NONCE_TYPE type) throws RuntimeException;

  /**
   * Get Id for nonce
   * @param nonce nonce
   * @param type  Nonce type that validates expiration
   * @return id matchin nonce
   * @throws RuntimeException on errors from underlying DAO
   * @throws StaleNonceException on time elapsed greater than expiration time set
   */
  public int getId(int nonce, NONCE_TYPE type) throws RuntimeException, StaleNonceException;
}
