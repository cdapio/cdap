package com.continuuity.passport.meta;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Store and provide roles and accounts corresponding to each of the roles.
 */
public class RolesAccounts {

  private Map<String, List<Account>> accountsRoles;

  public RolesAccounts() {
    this.accountsRoles = Maps.newHashMap();
  }

  /**
   * Add account role to the instance.
   * @param role Role
   * @param account Account
   */
  public void addAccountRole(String role, Account account){
    if (accountsRoles.containsKey(role)) {
      accountsRoles.get(role).add(account);
    } else {
      List<Account> accounts = Lists.newArrayList();
      accounts.add(account);
      accountsRoles.put(role, accounts);
    }
  }

  /**
   * Get all the roles.
   * @return Set of String representing roles
   */
  public Set<String> getRoles() {
    return accountsRoles.keySet();
  }

  /**
   * getAccounts for role
   * @param role Role
   * @return List of {@code Account}s
   */
  public List<Account> getAccounts(String role){
    if (accountsRoles.containsKey(role)){
      return accountsRoles.get(role);
    } else {
      return Lists.newArrayList();
    }
  }
}
