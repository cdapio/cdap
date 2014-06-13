/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

/**
 *
 */
public class Role {

  private final String roleType;
  private final String roleName;
  private final String permissions;

  public Role(String roleType, String roleName, String permissions) {
    this.roleType = roleType;
    this.roleName = roleName;
    this.permissions = permissions;
  }

  public Role(String roleName, String permissions) {
    this("", roleName, permissions);
  }

  public String getRoleType() {
    return roleType;
  }

  public String getRoleName() {
    return roleName;
  }

  public String getPermissions() {
    return permissions;
  }
}
