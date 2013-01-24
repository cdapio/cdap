package com.continuuity.passport.core;

/**
 * Defines Access control for each object
 */
public class ComponentACL {
  public enum ACL { READ, READ_WRITE, READ_WRITE_DELETE}
  private ACL acl;
  private String userId;

}
