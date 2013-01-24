package com.continuuity.passport.impl;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.service.PassportService;

/**
 * PassportService implementation
 */
public class PassportServiceImpl implements PassportService {
  @Override
  public boolean registerAccount(Account account, User owner) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean deleteAccount(String accountId, Credentials credentials) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean authenticateUser(Credentials credentials) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean authenticateComponent(String accountId, String componentId, Credentials credentials)
                                                                            throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean registerComponents(String accountId, Credentials credentials, Component component)
                                                                            throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean unRegisterComponent(String accountId, Credentials credentials, Component component)
                                                                            throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean updateComponent(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
