package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;

import java.lang.RuntimeException;


/**
 * Interface to manage various entities and components in the continuuity system
 *
 */

public interface Service {

  /**
   * Register an {@code Account} in the system
   * @param account  Account information
   * @param owner  Owner of the account
   * @return boolean  status of registration
   * @throws RuntimeException
   */
  public boolean registerAccount(Account account, User owner) throws RuntimeException;


  /**
   * Delete an {@code Account} in the system
   * @param accountId account to be deleted
   * @param credentials credentials of the owner of the account
   * @return boolean status of deletion
   * @throws RuntimeException
   */

  public boolean deleteAccount (String accountId, Credentials credentials) throws RuntimeException;

  /**
   * Authenticate an {@code User} in the system
   * @param credentials user credentials
   * @return boolean authentication status
   * @throws RuntimeException
   */
  public boolean authenticateUser ( Credentials credentials) throws RuntimeException;


  /**
   * Authenticate a component of an account by the user
   * @param accountId   account id identifying the account
   * @param credentials login credentials
   * @param componentId
   * @return boolean status of authentication
   * @throws RuntimeException
   */

  public boolean authenticateComponent ( String accountId, String componentId, Credentials credentials) throws RuntimeException;



  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   * @param accountId
   * @param credentials
   * @param component
   * @return boolean registration status
   * @throws RuntimeException
   */
  public boolean registerComponents( String accountId, Credentials credentials, Component component) throws RuntimeException;

  /**
   *  Unregister a {@code Component} in the system
   * @param accountId
   * @param credentials
   * @param component
   * @return boolean status of un-registering component
   * @throws RuntimeException
   */
  public boolean unRegisterComponent (String accountId, Credentials credentials, Component component) throws RuntimeException;


  /**
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return updateStatus
   * @throws RuntimeException
   */
  public boolean updateComponent( String accountId, Credentials credentials, Component component ) throws RuntimeException;



}
