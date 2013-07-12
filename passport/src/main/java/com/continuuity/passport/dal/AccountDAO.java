/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Role;

import java.util.Map;


/**
 * Data Access interface for account.
 * Manage all account related activity
 */
public interface AccountDAO {

  /**
   * Create Account in the system.
   * @param account Instance of {@code Account}
   * @return instance of {@code Account} that is created
   * @throws AccountAlreadyExistsException if the account to be created already exists
   */
  public Account createAccount(Account account) throws AccountAlreadyExistsException;


  public boolean confirmRegistration(Account account, String password);


  /**
   * Confirm Download when a user downloads developer suite.
   * @param accountId account id
   */
  public void confirmDownload(int accountId);

  /**
   * @param accountId account id
   * @param paymentId id in the external system
   */
  public void confirmPayment(int accountId, String paymentId);

  /**
   * Delete Account in the system.
   * @param accountId AccountId to be deleted
   * @return boolean status of account deletion, true on successful deletion, false otherwise
   * @throws AccountNotFoundException if the account to be deleted doesn't exist in the system
   */
  public boolean deleteAccount(int accountId) throws AccountNotFoundException;

  /**
   * GetAccount.
   * @param accountId AccountId requested
   * @return {@code Account}
   */
  public Account getAccount(int accountId);

  /**
   * Update organization if for the account.
   * @param accountId account id to be updated.
   * @param orgId Organization id to be updated.
   * @throws AccountNotFoundException on account to be updated not found.
   * @throws OrganizationNotFoundException on organization to be updated not found.
   */
  public void updateOrganizationId(int accountId, String orgId)
    throws AccountNotFoundException, OrganizationNotFoundException;

  /**
   * GetAccount based on email id.
   * @param emailId emailId of the account
   * @return Account
   */
  public Account getAccount(String emailId);

  public boolean addRoleType(int accountId, Role role);

  public void updateAccount(int accountId, Map<String, Object> keyValueParams);

  public void changePassword(int accountId, String oldPassword, String newPassword);

  public Account resetPassword(int nonce, String newPassword);

  public void regenerateApiKey(int accountId);

}
