package com.continuuity.passport.core.meta;

/**
 *
 */
public class VPCRole {

  private final VPC vpc;
  private final Role role;

  public VPCRole(VPC vpc, Role role) {
    this.vpc = vpc;
    this.role = role;
  }

  public VPC getVpc() {
    return vpc;
  }

  public Role getRole() {
    return role;
  }
}
