package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
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
   * @return boolean status of account creation
   * @throws {@code RetryException}
   */
  public boolean createAccount(Account account) throws ConfigurationException, RuntimeException;


  public boolean confirmRegistration(AccountSecurity security) throws ConfigurationException, RuntimeException;

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  public boolean deleteAccount(String accountId) throws ConfigurationException, RuntimeException;

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(int accountId) throws ConfigurationException, RuntimeException;


  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo) throws ConfigurationException, RuntimeException;

  /**
   * Configure the Data access objects
   * @param configurations Key value params for configuring the DAO
   */
  public void configure (Map<String,String> configurations);

  public boolean addRoleType(int accountId, Role role) throws ConfigurationException, RuntimeException;

}
