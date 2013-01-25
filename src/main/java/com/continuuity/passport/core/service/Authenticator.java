package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;

/**
 *  Interface for user authentication
 *  Use this interface to implement different kinds of authentication mechanisms
 */
public interface Authenticator {

  /**
   * Authenticates User with the Credentials passed
   * @param user User to be authenticated
   * @param credentials UserCredentials that authenticates the user
   * @return
   */
  boolean authenticate (User user, Credentials credentials);


}
