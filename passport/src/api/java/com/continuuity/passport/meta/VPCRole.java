/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

/**
 *
 */
public class VPCRole {

  private final VPC vpc;
  private final com.continuuity.passport.meta.Role role;

  public VPCRole(VPC vpc, com.continuuity.passport.meta.Role role) {
    this.vpc = vpc;
    this.role = role;
  }

  public VPC getVpc() {
    return vpc;
  }

  public com.continuuity.passport.meta.Role getRole() {
    return role;
  }
}
