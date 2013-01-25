package com.continuuity.passport.impl;

import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.status.AuthenticationStatus;

/**
 * Implementation of Authenticator that authenticates for Simple UserNamePassword authentication
 */
public class UsernamePasswordAuthenticatorImpl implements Authenticator {

  /**
   * Authenticates User with the Credentials passed
   *
   * @param user        User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  @Override
  public AuthenticationStatus authenticate(User user, Credentials credentials) throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
