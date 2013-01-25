package com.continuuity.passport.core;


/**
 * Manage account related functions
 */

public interface AccountManager {

  /**
   * Register account in the system
   * @param account Instance of {@code Account} to be registered
   * @param owner Instance of {@code Owner} of the account
   * @return
   * @throws RuntimeException
   */
  public  boolean registerAccount(Account account, User owner) throws RuntimeException;

  /**
   * Delete Account
   * @param accountId accountId
   * @param credentials Instance of {@code Credentials}
   * @return
   * @throws RuntimeException
   */
  public boolean deleteAccount (String accountId, Credentials credentials) throws RuntimeException;

  /**
   * Register Component in the system
   * @param accountId AccountId
   * @param credentials Instance of {@code Credentials} of the owner or admin
   * @param component Instance of {@code Component} to be registered
   * @return
   * @throws RuntimeException
   */
  public boolean registerComponents( String accountId, Credentials credentials, Component component) throws RuntimeException;

  /**
   * Update Component in the System
   * @param accountId  AccountId
   * @param credentials Instance of {@code Credentials} of the owner/admin to update the Component
   * @param component Instance of {@code Component}
   * @return
   * @throws RuntimeException
   */
  public boolean updateComponent( String accountId, Credentials credentials, Component component ) throws RuntimeException;


}
