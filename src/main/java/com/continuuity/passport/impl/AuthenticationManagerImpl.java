package com.continuuity.passport.impl;

import com.continuuity.passport.core.AuthenticationManager;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.data.access.layer.AccountDAO;
import com.continuuity.passport.data.access.layer.UserDAO;
import com.continuuity.passport.data.access.layer.db.AccountDBAccess;
import com.continuuity.passport.data.access.layer.db.UserDBAccess;

import java.util.Map;

/**
 * Authentication manager for user and account
 * Manages all the authentication request from the passport service
 */
public class AuthenticationManagerImpl implements AuthenticationManager{

  private AccountDAO accountDAO;
  private UserDAO userDAO;

  public AuthenticationManagerImpl(Map<String,String> daoConfig) {
    accountDAO = new AccountDBAccess(daoConfig);
    userDAO =  new UserDBAccess(daoConfig);
  }

  @Override
  public boolean authenticateUser(Credentials credentials) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean authenticateComponent(String accountId, String componentId, Credentials credentials) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
