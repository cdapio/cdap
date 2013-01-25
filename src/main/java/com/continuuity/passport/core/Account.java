package com.continuuity.passport.core;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;


/**
 * Account entity
 */

public class Account {


  private String accountId;

  private String name;


  private Set<User.UserRole> userRoles;

  private Set<Component> components = new HashSet<Component>();

  //TODO: Add billing info

  private void addUserRoles(User.UserRole role) {
    this.userRoles.add(role);
  }

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

  private void addComponent(Component c) {
    this.components.add(c);
  }

  public static class Builder {
    //TODO: Add Builders to create build the object
  }

}
