package com.continuuity.passport.dal.db;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.dal.AccountDAO;

import java.util.Map;

/**
 * AccountDAO implementation that uses database as the persistence store
 */
public class AccountDBAccess implements AccountDAO {

  private final Map<String, String> configuration;

  public AccountDBAccess(final Map<String, String> configuration) {
    this.configuration = configuration;
  }

  /**
   * Create Account in the system
   *
   * @param accountId accountID
   * @param account   Instance of {@code Account}
   * @return boolean status of account creation
   * @throws {@code RetryException}
   */
  @Override
  public boolean createAccount(String accountId, Account account) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  @Override
  public boolean deleteAccount(String accountId) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  @Override
  public Account getAccount(String accountId) throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * AccountId to be updated
   *
   * @param accountId AccountId
   * @param account   Instance of {@code Account}
   * @return boolean status of account update
   * @throws {@code RetryException}
   */
  @Override
  public boolean updateAccount(String accountId, Account account) throws RetryException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
