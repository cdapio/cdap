package com.continuuity.passport.dal;

import com.continuuity.passport.core.Account;


/**
 *     Data Access interface for account
 *     Manage all account related activity
 */
public interface AccountDAO {

  /**
   * Create Account in the system
   * @param accountId accountID
   * @param account Instance of {@code Account}
   * @return
   * @throws RuntimeException
   */
  public boolean createAccount(String accountId, Account account) throws RuntimeException;

  /**
   * Delete Account in the system
   * @param accountId AccountId to be deleted
   * @return
   * @throws RuntimeException
   */
  public boolean deleteAccount(String accountId) throws RuntimeException;

  /**
   * GetAccount
   * @param accountId AccountId requested
   * @return
   * @throws RuntimeException
   */
  public Account getAccount(String accountId) throws RuntimeException;


  /**
   * AccountId to be updated
   * @param accountId AccountId
   * @param account Instance of {@code Account}
   * @return
   * @throws RuntimeException
   */
  public Account updateAccount(String accountId, Account account) throws RuntimeException;


}
