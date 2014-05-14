/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.StaleNonceException;

/**
 * Manage Account activation and session nonce.
 */
public interface NonceDAO {

  /**
   * Nonce type - possible values: SESSION - to create nonce to manage session
   *  ACTIVATION - for account activation, RESET - to reset password.
   */
  public enum NONCE_TYPE {
    SESSION,
    ACTIVATION,
    RESET
  }

  /**
   * Generates random nonce for id.
   * @param id   id to be nonced
   * @param type Nonce type that determines expiration
   * @return random nonce
   * @throws RuntimeException on errors from underlying DAO
   */
  public int getNonce(String id, NONCE_TYPE type);

  // Due to a bug in checkstyle, it would emit false positives here of the form
  // "Unable to get class information for @throws tag '<exn>' (...)".
  // This comment disables that check up to the corresponding ON comments below

  // CHECKSTYLE OFF: @throws

  /**
   * Get Id for nonce.
   * @param nonce nonce
   * @param type  Nonce type that validates expiration
   * @return id matchin nonce
   * @throws RuntimeException    on errors from underlying DAO
   * @throws StaleNonceException on time elapsed greater than expiration time set
   */
  public String getId(int nonce, NONCE_TYPE type) throws StaleNonceException;

  // CHECKSTYLE ON

}
