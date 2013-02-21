package com.continuuity.passport.meta;

/**
 * Defines Access control for each object
 */
public class ComponentACL {
  public enum Type {READ, READ_WRITE, READ_WRITE_DELETE}

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
