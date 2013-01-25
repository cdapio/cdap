package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.status.AuthenticationStatus;

/**
 *  Interface for user authentication
 *  Use this interface to implement different kinds of authentication mechanisms
 */
public interface Authenticator {

  /**
   * Authenticates User with the Credentials passed
   * @param user User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  AuthenticationStatus authenticate (User user, Credentials credentials) throws RetryException;


}
