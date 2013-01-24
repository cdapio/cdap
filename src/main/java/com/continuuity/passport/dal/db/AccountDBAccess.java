package com.continuuity.passport.dal.db;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.dal.AccountDAO;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 *  AccountDAO implementation that uses database as the persistence store
 */
public class AccountDBAccess implements AccountDAO {

 private final Map<String,String> configuration;

  public AccountDBAccess(final Map<String, String> configuration){
    this.configuration = configuration;
  }

  /**
   * Create Account in the system
   *
   * @param accountId accountID
   * @param account   Instance of {@code Account}
   * @return
   * @throws RuntimeException
   */
  @Override
  public boolean createAccount(String accountId, Account account) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return
   * @throws RuntimeException
   */
  @Override
  public boolean deleteAccount(String accountId) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return
   * @throws RuntimeException
   */
  @Override
  public Account getAccount(String accountId) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * AccountId to be updated
   *
   * @param accountId AccountId
   * @param account   Instance of {@code Account}
   * @return
   * @throws RuntimeException
   */
  @Override
  public Account updateAccount(String accountId, Account account) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
