package com.continuuity.passport.core.service;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Credentials;
import com.continuuity.passport.core.status.AuthenticationStatus;

import java.util.Map;

/**
 * Interface for user authentication
 * Use this interface to implement different kinds of authentication mechanisms
 */
public interface Authenticator {

  /**
   * Authenticates User with the Credentials passed
   *
   * @param userId        User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  AuthenticationStatus authenticate(Credentials credentials) throws RetryException;

  public void configure (Map<String,String> configurations);

}
