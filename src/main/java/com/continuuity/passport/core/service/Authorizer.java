package com.continuuity.passport.core.service;

import com.continuuity.passport.core.Account;
import com.continuuity.passport.core.Component;
import com.continuuity.passport.core.ComponentACL;
import com.continuuity.passport.core.Credentials;
import com.continuuity.passport.core.User;
import com.continuuity.passport.core.exceptions.RetryException;

/**
 *
 */
public interface Authorizer {

  /**
   * Authorize component for the user with the request ACLType
   * Example: Authorize User: Foo to DataSet: Bar with ACL: READ
   *
   * @param user        User requesting authorization
   * @param account     Account that owns the dataSet
   * @param component   Component for which authorization is requested
   * @param aclType     ACL requested on the component
   * @param credentials UserCredentials that authenticates the user
   * @return Instance of {@code DelegationToken}
   */
  DelegationToken authorize(User user, Account account, Component component, ComponentACL.Type aclType,
                            Credentials credentials) throws RetryException;

  /**
   * DelegationToken class - to grant access to ACLs
   */
  class DelegationToken {

    public enum AccessType {ACCESS, NO_ACCESS}

    private AccessType type;

    private String userId;

    private String accountId;

    private String componentId;

    private ComponentACL.Type acl;

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
