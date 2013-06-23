package com.continuuity.passport.core.service;

import com.continuuity.passport.core.exceptions.StaleNonceException;

/**
 * Service that manages security for single sign on and account activation.
 */
public interface SecurityService {
  /**
   * Generate a unique id to be used in activation email to enable secure (nonce based) registration process.
   * @param id Id to be nonced
   * @return random nonce
   */
  public int getActivationNonce(String id);

  /**
   * Get id for nonce.
   * @param nonce nonce that was generated.
   * @return id corresponding to the nonce that was generated
   * @throws StaleNonceException on calling getActivationId with nonce that has expired.
   */
  public String getActivationId(int nonce) throws StaleNonceException;

  /**
   * Generate a nonce that will be used for sessions.
   * @param id ID to be nonced
   * @return random nonce
   */
  public int getSessionNonce(String id);

  /**
   * Get id for nonce.
   * @param nonce
   * @return account id for nonce key
   * @throws StaleNonceException on nonce that was generated expiring in the system
   */
  public String getSessionId(int nonce) throws StaleNonceException;

  /**
   * Generate Reset Nonce.
   * @param id Id
   * @return random nonce
   */
  public int getResetNonce(String id);
}
