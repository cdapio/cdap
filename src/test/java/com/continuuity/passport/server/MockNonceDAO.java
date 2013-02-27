package com.continuuity.passport.server;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.dal.NonceDAO;

/**
 * MockNonceDAO used for testing passport. Stores data in HashMap and returns them back.
 * Note: TODO: This is not fully implmented yet.
 */
public class MockNonceDAO implements NonceDAO {

  /**
   * Generates random nonce for id
   *
   * @param id   id to be nonced
   * @param type Nonce type that determines expiration
   * @return random nonce
   * @throws RuntimeException on errors from underlying DAO
   */
  @Override
  public int getNonce(String id, NONCE_TYPE type) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Get Id for nonce
   *
   * @param nonce nonce
   * @param type  Nonce type that validates expiration
   * @return id matchin nonce
   * @throws RuntimeException on errors from underlying DAO
   * @throws com.continuuity.passport.core.exceptions.StaleNonceException
   *                          on time elapsed greater than expiration time set
   */
  @Override
  public String getId(int nonce, NONCE_TYPE type) throws StaleNonceException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
