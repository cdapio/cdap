package com.continuuity.passport.core;

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
    return this.userRoles;
  }

  public Set<Component> getComponents() {
    return this.getComponents();
  }

  private void addComponent(Component c) {
    this.components.add(c);
  }

   public static class Builder {
    //TODO: Add Builders to create build the object
   }

}
