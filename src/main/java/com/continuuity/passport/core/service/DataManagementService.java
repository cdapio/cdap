package com.continuuity.passport.core.service;


import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.Component;
import com.continuuity.passport.core.meta.Credentials;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.status.Status;

import java.util.List;
import java.util.Map;

/**
 *
 */
public interface DataManagementService {

  /**
   * Register an {@code Account} in the system
   *
   * @param account Account information
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Account registerAccount(Account account) throws RuntimeException;

  public long addVPC(int accountId, VPC vpc) throws RuntimeException;


  public Status confirmRegistration(AccountSecurity account) throws RuntimeException;

  public void confirmDownload(int accountId) throws RuntimeException;

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status registerComponents(String accountId, Credentials credentials,
                                   Component component) throws RetryException;

  /**
   * Unregister a {@code Component} in the system
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status unRegisterComponent(String accountId, Credentials credentials,
                                    Component component) throws RetryException;

  /**
   * Delete an {@code Account} in the system
   *
   * @param accountId   account to be deleted
   * @param credentials credentials of the owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status deleteAccount(String accountId, Credentials credentials) throws RetryException;


  /**
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status updateComponent(String accountId, Credentials credentials, Component component) throws RetryException;


  /**
   * GetAccount object
   *
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  public Account getAccount(int accountId) throws RuntimeException;

  /**
   * Get VPC list for accountID
   * @param accountId accountId identifying accounts
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(int accountId);

  /**
   * Get VPC List based on the ApiKey
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(String apiKey);

  /**
   * Update account with passed Params
   * @param accountId accountId
   * @param params  Map<"keyName", "value">
   */
  public void updateAccount(int accountId, Map<String,Object> params) throws RuntimeException;


}
