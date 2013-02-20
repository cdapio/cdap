package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

import java.util.Map;

/**
 *  Implementation of Authentication Service
 */

public class AuthenticatorServiceImpl implements AuthenticatorService {

  private final Map<String, String> configuration;


  @Inject
  public AuthenticatorServiceImpl(@Named("passport.config") Map<String, String> config) {
    this.configuration = config;
  }


  /**
   * Authenticates User with the Credentials passed
   *
   * @param credentials UserCredentials that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  @Override
  public AuthenticationStatus authenticate(Credentials credentials) throws RetryException {

    UsernamePasswordApiKeyToken userCredentials = (UsernamePasswordApiKeyToken) credentials;
    try {
      Subject currentUser = SecurityUtils.getSubject();
      currentUser.login(userCredentials);
      Account account = (Account) currentUser.getPrincipal();
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATED, account.toString());
    } catch (Exception e) {
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATION_FAILED,
        "Authentication Failed. " + e.getMessage());

    }

  }
}
