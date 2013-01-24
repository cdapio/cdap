package com.continuuity.passport.data.access.layer;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;

import java.util.Set;


/**
 *     Data Access interface for account
 *     Manage all account related activity
 */
public interface AccountDAO {

  public boolean createAccount(String accountId, Account account) throws RuntimeException;

  public boolean addComponent(String account, Component component) throws RuntimeException;

  public boolean removeComponent(Account account, Component component) throws RuntimeException;

  public Set<Component> getComponents(String accountId);

  public boolean deleteAccount(Account account) throws RuntimeException;

}
