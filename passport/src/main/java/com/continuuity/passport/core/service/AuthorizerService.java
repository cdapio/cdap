/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.service;

import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.ComponentACL;

/**
 * Authorize Components and ACLs.
 * This is not implemented yet for initial use case
 */
public interface AuthorizerService {

  /**
   * Authorize component for the user with the request ACLType.
   * Example: Authorize User: Foo to DataSet: Bar with ACL: READ
   *
   * @param user        User requesting authorization
   * @param account     Account that owns the dataSet
   * @param component   Component for which authorization is requested
   * @param aclType     ACL requested on the component
   * @param credentials UserCredentials that authenticates the user
   * @return Instance of {@code DelegationToken}
   */
  DelegationToken authorize(String user, Account account, Component component, ComponentACL.Type aclType,
                            Credentials credentials) throws RetryException;

  /**
   * DelegationToken class - to grant access to ACLs.
   */
  class DelegationToken {

    public enum AccessType { ACCESS, NO_ACCESS }

    private final AccessType type;

    private final String userId;

    private final String accountId;

    private final String componentId;

    private final ComponentACL.Type acl;

    public DelegationToken(String userId, String accountId, String componentId, ComponentACL.Type acl,
                           AccessType accessType) {
      this.type = accessType;
      this.userId = userId;
      this.accountId = accountId;
      this.componentId = componentId;
      this.acl = acl;
    }

    public AccessType getType() {
      return type;
    }

    public String getUserId() {
      return userId;
    }

    public String getAccountId() {
      return accountId;
    }

    public String getComponentId() {
      return componentId;
    }

    public ComponentACL.Type getAcl() {
      return acl;
    }
  }
}
