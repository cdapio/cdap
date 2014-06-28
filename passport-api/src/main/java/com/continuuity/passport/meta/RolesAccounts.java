package com.continuuity.passport.meta;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Store and provide roles and accounts corresponding to each of the roles.
 */
public final class RolesAccounts {

  private final Multimap<String, Account> accountsRoles;

  public RolesAccounts() {
    this.accountsRoles = ArrayListMultimap.create();
  }

  /**
   * Add account role to the instance.
   * @param role Role
   * @param account Account
   */
  public void addAccountRole(String role, Account account) {
    accountsRoles.put(role, account);
  }

  /**
   * Get all the roles.
   * @return Set of String representing roles
   */
  public Set<String> getRoles() {
    return Collections.unmodifiableSet(accountsRoles.keySet());
  }

  /**
   * getAccounts for role.
   * @param role Role
   * @return List of {@code Account}s
   */
  public Collection<Account> getAccounts(String role) {
    return Collections.unmodifiableCollection(accountsRoles.get(role));
  }
}
