package com.continuuity.passport.impl;

import com.continuuity.passport.core.*;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.UserDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.UserDBAccess;

import java.util.Map;

/**
 * AccountManager manages all the operations from the Passport Service
 * This implementation is specific to Database Data acccessObjects
 */
public class AccountManagerImpl implements  AccountManager {

  private AccountDAO accountDAO;
  private UserDAO userDAO;

  public AccountManagerImpl (Map<String,String> daoConfig) {
    accountDAO = new AccountDBAccess(daoConfig);
    userDAO =  new UserDBAccess(daoConfig);
  }

  @Override
  public boolean registerAccount(Account account, User owner) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean deleteAccount(String accountId, Credentials credentials) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean registerComponents(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean updateComponent(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
