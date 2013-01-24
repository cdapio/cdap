package com.continuuity.passport.data.access.layer.db;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.data.access.layer.AccountDAO;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AccountDBAccess implements AccountDAO {

 private Map<String,String> configuration;

  public AccountDBAccess(Map<String, String> configuration){
    this.configuration = configuration;
  }

  @Override
  public boolean createAccount(String accountId, Account account) throws RuntimeException {
    /**
     * Access pattern
     * DBConnectionManager.getInstance().getConnection();
     * use the connection to execute
     * getConnection() method is synchronized
     */
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean addComponent(String account, Component component) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean removeComponent(Account account, Component component) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<Component> getComponents(String accountId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean deleteAccount(Account account) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
