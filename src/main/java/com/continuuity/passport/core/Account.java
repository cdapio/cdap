package com.continuuity.passport.core;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;


/**
 * Account entity
 */

public class Account {


  private final String accountId;

  private final String name;

  private final Set<User.UserRole> userRoles;

  private final Set<Component> components = new HashSet<Component>();

  public Account( final String accountId,  final String name,  final Set<User.UserRole> userRoles) {
    this.accountId = accountId;
    this.name = name;
    this.userRoles = userRoles;
  }

  //TODO: Add billing info

  public Set<User.UserRole> getUserRoles() {
    return ImmutableSet.copyOf(this.userRoles);
  }

  /**
   * getComponents for the account
   *
   * @return ImmutableSet of Component
   */
  public Set<Component> getComponents() {
    return ImmutableSet.copyOf(this.getComponents());
  }


  public static class Builder {
    //TODO: Add Builders to create build the object
  }

}
