/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import co.cask.cdap.api.security.Principals;
import co.cask.cdap.security.exception.UnauthorizedException;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import javax.inject.Inject;

/**
 * Provides ways to authorize requests.
 */
public class AuthorizationClient {

  private final ACLClient aclClient;

  @Inject
  public AuthorizationClient(final ACLClient aclClient) {
    this.aclClient = aclClient;
  }

  /**
   * Authorizes a user's access to an entity.
   *
   * @param userId id of the user
   * @param entityId id of the entity
   * @param requiredPermissions permissions required to access the entity
   */
  public void authorizeUser(String userId, EntityId entityId, Iterable<PermissionType> requiredPermissions) {
    try {
      if (!isUserAuthorized(userId, entityId, requiredPermissions)) {
        throw new UnauthorizedException("Not authorized");
      }
    } catch (IOException e) {
      throw new UnauthorizedException("Not authorized");
    }
  }

  private boolean isUserAuthorized(String userId, EntityId entityId,
                                   Iterable<PermissionType> requiredPermissions) throws IOException {

    List<ACL> entityAcls = aclClient.listAcls(entityId);
    if (entityAcls == null || entityAcls.isEmpty()) {
      return false;
    }

    // first check user
    ACL userAcl = getPrincipalAcl(entityAcls, new Principal(PrincipalType.USER, userId));
    if (userAcl != null) {
      // if user ACL exists, then only use the user ACL to check for permissions
      return hasRequiredPermissions(userAcl, requiredPermissions);
    }

    // then check user's groups
    List<String> userGroupStrings = Lists.newArrayList();
    List<Principal> userGroups = Principals.fromIds(PrincipalType.GROUP, userGroupStrings);
    return isAuthorized(userGroups, entityAcls, requiredPermissions);
  }

  /**
   * @param principals principals to authorize
   * @param acls ACLs to check for the requiredPermissions
   * @param requiredPermissions permissions that are required for authorization
   * @return true, if there is an ACL that meets the requiredPermissions for at least one of the principals
   *         false, if there is no ACL that meets the requiredPermissions for any of the principals
   */
  private boolean isAuthorized(List<Principal> principals, List<ACL> acls,
                               Iterable<PermissionType> requiredPermissions) {
    for (Principal principal : principals) {
      for (ACL acl : acls) {
        if (principal.equals(acl.getPrincipal()) && hasRequiredPermissions(acl, requiredPermissions)) {
          return true;
        }
      }
    }
    return false;
  }

  private ACL getPrincipalAcl(List<ACL> acls, Principal principal) {
    for (ACL acl : acls) {
      if (principal.equals(acl.getPrincipal())) {
        return acl;
      }
    }
    return null;
  }

  private boolean hasRequiredPermissions(ACL acl, Iterable<PermissionType> requiredPermissions) {
    for (PermissionType requiredPermission : requiredPermissions) {
      if (!acl.getPermissions().contains(requiredPermission)) {
        return false;
      }
    }
    return true;
  }
}
