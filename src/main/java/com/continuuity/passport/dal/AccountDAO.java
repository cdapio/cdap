package com.continuuity.passport.dal;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.exceptions.RetryException;


/**
 *     Data Access interface for account
 *     Manage all account related activity
 */
public interface AccountDAO {

  /**
   * Create Account in the system
   * @param accountId accountID
   * @param account Instance of {@code Account}
   * @return boolean status of account creation
   * @throws {@code RetryException}
   */
  public boolean createAccount(String accountId, Account account) throws RetryException;

  /**
   * Delete Account in the system
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  public boolean deleteAccount(String accountId) throws RetryException;

  /**
   * GetAccount
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(String accountId) throws RetryException;


  /**
   * AccountId to be updated
   * @param accountId AccountId
   * @param account Instance of {@code Account}
   * @return boolean status of update
   * @throws {@code RetryException}
   */
  public boolean updateAccount(String accountId, Account account) throws RetryException;


}
