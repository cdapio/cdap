/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.BillingInfo;
import com.continuuity.passport.meta.Role;

import java.util.Map;


/**
 * Data Access interface for account
 * Manage all account related activity
 */
public interface AccountDAO {

  /**
   * Create Account in the system
   *
   * @param account Instance of {@code Account}
   * @return int account Id that was generated
   * @throws {@code RetryException}
   */
  public Account createAccount(Account account) throws ConfigurationException, AccountAlreadyExistsException;


  public boolean confirmRegistration(Account account, String password) throws ConfigurationException;


  /**
   * @param accountId
   * @throws ConfigurationException
   * @throws RuntimeException
   */
  public void confirmDownload(int accountId) throws ConfigurationException;


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
  public Account getAccount(int accountId) throws ConfigurationException;

  /**
   * GetAccount
   *
   * @param emailId emailId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(String emailId) throws ConfigurationException;


  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo)
    throws ConfigurationException;


  public boolean addRoleType(int accountId, Role role) throws ConfigurationException;

  public void updateAccount(int accountId, Map<String, Object> keyValueParams)
    throws ConfigurationException;

  public void changePassword(int accountId, String oldPassword, String newPassword);

  public Account resetPassword(int nonce, String newPassword);

  public void regenerateApiKey(int accountId);

}
