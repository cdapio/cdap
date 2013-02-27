package com.continuuity.passport.server;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.meta.Account;

/**
 *
 */
public class MockAuthenticatorImpl implements AuthenticatorService {

  /**
   * Authenticates User with the Credentials passed
   *
   * @param credentials {@code Credentials} that authenticates the user
   * @return {@code AuthenticationStatus}
   * @throws {@code RetryException}
   */
  @Override
  public AuthenticationStatus authenticate(Credentials credentials) throws RetryException {
    UsernamePasswordApiKeyToken userCredentials = (UsernamePasswordApiKeyToken) credentials;
    Account account = (Account) MockAccountDAO.authenticate(userCredentials.getApiKey());
    return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATED, account.toString());
  }
}
