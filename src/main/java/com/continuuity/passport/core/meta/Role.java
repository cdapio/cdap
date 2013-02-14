package com.continuuity.passport.core.meta;

/**
 *
 */
public class Role {

  private final int roleId;
  private final String roleName;
  private final String permissions;

  public Role(int roleId, String roleName, String permissions) {
    this.roleId = roleId;
    this.roleName = roleName;
    this.permissions = permissions;
  }

  public Role(String roleName, String permissions) {
    this(-1,roleName,permissions);
  }

  public int getRoleId() {
    return roleId;
  }

  public String getRoleName() {
    return roleName;
  }

  public String getPermissions() {
    return permissions;
  }
}
