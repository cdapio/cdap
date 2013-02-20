package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.BillingInfo;
import com.continuuity.passport.core.meta.Role;

import java.util.Map;


/**
 * Data Access interface for account
 * Manage all account related activity
 */
public interface AccountDAO {

  /**
   * Create Account in the system
   *
   * @param account   Instance of {@code Account}
   * @return int account Id that was generated
   * @throws {@code RetryException}
   */
  public Account createAccount(Account account) throws ConfigurationException, RuntimeException, AccountAlreadyExistsException;


  public boolean confirmRegistration(Account account, String password) throws ConfigurationException, RuntimeException;


  /**
   * @param accountId
   * @return
   * @throws ConfigurationException
   * @throws RuntimeException
   */
  public void confirmDownload(int accountId) throws ConfigurationException, RuntimeException;


  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  public boolean deleteAccount(int accountId)
                        throws ConfigurationException, RuntimeException, AccountNotFoundException;

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(int accountId) throws ConfigurationException, RuntimeException;

  /**
   * GetAccount
   *
   * @param emailId emailId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(String emailId) throws ConfigurationException, RuntimeException;


  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo)
                                                                  throws ConfigurationException, RuntimeException;


  public boolean addRoleType(int accountId, Role role) throws ConfigurationException, RuntimeException;

  public void updateAccount(int accountId, Map<String,Object> keyValueParams)
    throws ConfigurationException, RuntimeException;

  public void changePassword(int accountId, String oldPassword, String newPassword) throws RuntimeException;
}
