package com.continuuity.passport.core;

/**
 * Manage authentication Services
 */

public interface AuthenticationManager  {

  public boolean authenticateUser ( Credentials credentials) throws RuntimeException;

  public boolean authenticateComponent ( String accountId, String componentId, Credentials credentials) throws RuntimeException;

}
