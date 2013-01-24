package com.continuuity.passport.impl;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.service.Status;

/**
 *
 */
public class DataManagementServiceImpl implements DataManagementService {


  /**
   * Register an {@code Account} in the system
   *
   * @param account Account information
   * @param owner   Owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status registerAccount(Account account, User owner) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status registerComponents(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Unregister a {@code Component} in the system
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status unRegisterComponent(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete an {@code Account} in the system
   *
   * @param accountId   account to be deleted
   * @param credentials credentials of the owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status deleteAccount(String accountId, Credentials credentials) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component) throws RuntimeException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * get User Object
   *
   * @param userId Id that defines the user
   * @return Instance of {@code User}
   */
  @Override
  public User getUser(String userId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount object
   *
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(String accountId) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
