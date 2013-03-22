/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
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
   * @throws {@code AccountAlreadyExistsException}
   */
  public Account createAccount(Account account) throws AccountAlreadyExistsException;


  public boolean confirmRegistration(Account account, String password);


  /**
   * @param accountId
   * @throws RuntimeException
   */
  public void confirmDownload(int accountId);

  /**
   * @param accountId
   * @param paymentId id in the external system
   * @throws RuntimeException
   */
  public void confirmPayment(int accountId, String paymentId);

  /**
   * Delete Account in the system
   *
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion
   * @throws {@code RetryException}
   */
  public boolean deleteAccount(int accountId) throws  AccountNotFoundException;

  /**
   * GetAccount
   *
   * @param accountId AccountId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(int accountId);

  /**
   * GetAccount
   *
   * @param emailId emailId requested
   * @return {@code Account}
   * @throws {@code RetryException}
   */
  public Account getAccount(String emailId) ;


  public boolean updateBillingInfo(int accountId, BillingInfo billingInfo);

  public boolean addRoleType(int accountId, Role role);

  public void updateAccount(int accountId, Map<String, Object> keyValueParams);

  public void changePassword(int accountId, String oldPassword, String newPassword);

  public Account resetPassword(int nonce, String newPassword);

  public void regenerateApiKey(int accountId);

}
