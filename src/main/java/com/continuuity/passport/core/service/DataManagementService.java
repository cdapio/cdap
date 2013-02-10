package com.continuuity.passport.core.service;


import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.status.Status;

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
  public Status registerAccount(Account account) throws RetryException;

  public Status confirmRegistration(Account account) throws RetryException;

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
  public Account getAccount(String accountId);

}
