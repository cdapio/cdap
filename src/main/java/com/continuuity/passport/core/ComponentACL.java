package com.continuuity.passport.core;

/**
 * Defines Access control for each object
 */
public class ComponentACL {
  public enum Type { READ, READ_WRITE, READ_WRITE_DELETE}
  private Type acl;
  private String userId;

  public ComponentACL(Type acl, String userId) {
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
