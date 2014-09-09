/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.SecurityRequestContext;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;

/**
 * Annotation for a method that requires permissions to execute.
 */
public class RequiresPermissionsMethodInterceptor implements MethodInterceptor {

  private final Provider<CConfiguration> configuration;
  private final Provider<? extends ACLClient> aclClient;

  @Inject
  public RequiresPermissionsMethodInterceptor(Provider<? extends ACLClient> aclClient,
                                              Provider<CConfiguration> configuration) {
    this.aclClient = aclClient;
    this.configuration = configuration;
  }

  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    if (configuration.get().getBoolean(Constants.Security.CFG_SECURITY_AUTHORIZATION_ENABLED)) {
      RequiresPermissions requiresPermissions = getAnnotation(methodInvocation.getMethod().getDeclaredAnnotations());
      if (requiresPermissions != null) {
        Set<PermissionType> requiredPermissions = ImmutableSet.copyOf(requiresPermissions.value());
        String userId = SecurityRequestContext.getUserId();
        EntityId entityId = SecurityRequestContext.getEntityId();
        if (!isUserAuthorized(userId, entityId, requiredPermissions)) {
          throw new NotAuthorizedException("Not authorized");
        }
      }
    }
    return methodInvocation.proceed();
  }

  private boolean isUserAuthorized(String userId, EntityId entityId,
                                   Iterable<PermissionType> requiredPermissions) throws Exception {
    List<ACL> entityAcls = aclClient.get().listACLs(entityId);
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

  private RequiresPermissions getAnnotation(Annotation[] annotations) {
    for (Annotation annotation : annotations) {
      if (annotation instanceof RequiresPermissions) {
        return (RequiresPermissions) annotation;
      }
    }
    return null;
  }
}
