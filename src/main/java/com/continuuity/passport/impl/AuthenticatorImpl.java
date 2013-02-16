package com.continuuity.passport.impl;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.Credentials;
import com.continuuity.passport.core.meta.UsernamePasswordApiKeyCredentials;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.dal.db.DBUtils;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */

public class AuthenticatorImpl implements Authenticator {

  private static AuthenticatorImpl instance  = null;

  private AuthenticatorImpl(){

  }

  public static AuthenticatorImpl getInstance() {
    if ( instance == null) {
      instance = new AuthenticatorImpl();
    }
    return instance;
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
