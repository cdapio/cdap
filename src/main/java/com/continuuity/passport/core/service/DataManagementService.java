package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;

/**
 *
 */
public interface DataManagementService {

  /**
   * Register an {@code Account} in the system
   * @param account  Account information
   * @param owner  Owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status registerAccount(Account account, User owner) throws RuntimeException;


  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status registerComponents( String accountId, Credentials credentials, Component component) throws RuntimeException;

  /**
   *  Unregister a {@code Component} in the system
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status unRegisterComponent (String accountId, Credentials credentials, Component component) throws RuntimeException;

  /**
   * Delete an {@code Account} in the system
   * @param accountId account to be deleted
   * @param credentials credentials of the owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */

  public Status deleteAccount (String accountId, Credentials credentials) throws RuntimeException;


  /**
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  public Status updateComponent( String accountId, Credentials credentials, Component component ) throws RuntimeException;

  /**
   * get User Object
   * @param userId Id that defines the user
   * @return Instance of {@code User}
   */
  public User getUser(String userId) ;

  /**
   * GetAccount object
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  public Account getAccount (String accountId);

}
