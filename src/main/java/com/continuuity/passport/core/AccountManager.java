package com.continuuity.passport.core;


/**
 * Manage account related functions
 */

public interface AccountManager {

  public  boolean registerAccount(Account account, User owner) throws RuntimeException;

  public boolean deleteAccount (String accountId, Credentials credentials) throws RuntimeException;

  public boolean registerComponents( String accountId, Credentials credentials, Component component) throws RuntimeException;

  public boolean updateComponent( String accountId, Credentials credentials, Component component ) throws RuntimeException;


}
