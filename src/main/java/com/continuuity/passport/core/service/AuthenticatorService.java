package com.continuuity.passport.core.service;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.status.AuthenticationStatus;

/**
 * Interface for user authentication
 * Use this interface to implement different kinds of authentication mechanisms
 */
public interface AuthenticatorService {

  /**
   * Authenticates User with the Credentials passed
   *
   * @param credentials {@code Credentials} that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  AuthenticationStatus authenticate(Credentials credentials) throws RetryException;

}
