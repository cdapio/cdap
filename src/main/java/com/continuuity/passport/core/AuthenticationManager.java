package com.continuuity.passport.core;

/**
 * Manage authentication Services
 */

public interface AuthenticationManager  {

  /**
   * Authenticate User
   * @param credentials Instance of {@code Credentials}
   * @return
   * @throws RuntimeException
   */
  public boolean authenticateUser ( Credentials credentials) throws RuntimeException;

  /**
   * Authenticate Component
   * @param accountId
   * @param componentId
   * @param credentials
   * @return
   * @throws RuntimeException
   */
  public boolean authenticateComponent ( String accountId, String componentId, Credentials credentials) throws RuntimeException;

}
