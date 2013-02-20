package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.Credentials;
import com.continuuity.passport.core.meta.UsernamePasswordApiKeyCredentials;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;

import java.util.Map;

/**
 *
 */

public class AuthenticatorImpl implements Authenticator {

  private final Map<String,String> configuration;


  @Inject
  public AuthenticatorImpl(@Named("passport.config") Map<String,String> config) {
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

    UsernamePasswordApiKeyCredentials userCredentials = (UsernamePasswordApiKeyCredentials) credentials;

    UsernamePasswordApiKeyToken token  =  new UsernamePasswordApiKeyToken(userCredentials.getUserName(),
                                                                          userCredentials.getPassword(),
                                                                          userCredentials.getApiKey());

    try {
      Subject currentUser = SecurityUtils.getSubject();
      currentUser.login(token);
      Account account = (Account) currentUser.getPrincipal();
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATED,account.toString());
    }
    catch (Exception e){
      return new AuthenticationStatus(AuthenticationStatus.Type.AUTHENTICATION_FAILED,
                                      "Authentication Failed. "+e.getMessage());

    }

  }

  @Override
  public void configure(Map<String, String> configurations) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
