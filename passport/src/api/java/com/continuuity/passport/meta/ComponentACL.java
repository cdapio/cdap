/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

/**
 * Defines Access control for each object.
 */
public class ComponentACL {

  /**
   * Type of ComponentACL - Possible values - READ, READ_WRITE, READ_WRITE_DELETE.
   */
  public enum Type { READ, READ_WRITE, READ_WRITE_DELETE }

  private final Type acl;
  private final String userId;

  public ComponentACL(final Type acl, final String userId) {
    this.acl = acl;
    this.userId = userId;
  }

  public Type getAcl() {
    return acl;
  }

  public String getUserId() {
    return userId;
  }
}
