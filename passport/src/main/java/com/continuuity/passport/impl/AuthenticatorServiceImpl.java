/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.impl;

import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.meta.Account;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

/**
 * Implementation of Authentication Service.
 */
public class AuthenticatorServiceImpl implements AuthenticatorService {

  /**
   * Authenticates User with the Credentials passed.
   *
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   */
  @Override
  public AuthenticationStatus authenticate(Credentials credentials) {
    UsernamePasswordApiKeyToken userCredentials = (UsernamePasswordApiKeyToken) credentials;
    Subject currentUser = null;
    try {
      currentUser = SecurityUtils.getSubject();
      currentUser.login(userCredentials);
      Account account = (Account) currentUser.getPrincipal();
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATED, account.toString());
    } catch (Exception e) {
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATION_FAILED,
        "Authentication Failed. " + e.getMessage());
    } finally {
      currentUser.logout();
    }
  }
}
