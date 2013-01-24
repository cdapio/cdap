package com.continuuity.passport.impl;

import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.service.Authenticator;

/**
 * Implementation of Authenticator that authenticates for Simple UserNamePassword authentication
 */
public class UsernamePasswordAuthenticatorImpl implements Authenticator {

  /**
   * Authenticates User with the Credentials passed
   *
   * @param user        User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return
   */
  @Override
  public boolean authenticate(User user, Credentials credentials) {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
