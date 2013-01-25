package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.ComponentACL;

/**
 *  DelegationToken class - to grant access to ACLs
 */
public class DelegationToken {

  public enum AccessType {ACCESS, NO_ACCESS}

  private AccessType type;

  private String userId;

  private String accountId;

  private String componentId;

  private ComponentACL.Type acl;

  public DelegationToken(String userId, String accountId, String componentId, ComponentACL.Type acl, AccessType accessType) {
    this.type = accessType;
    this.userId = userId;
    this.accountId = accountId;
    this.componentId = componentId;
    this.acl = acl;
  }
}
