/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import co.cask.cdap.security.impersonation.SecurityUtil;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Utility functions for Authorization
 */
public class AuthorizationUtil {

  private AuthorizationUtil() { }

  /**
   * Ensures that the principal has at least one {@link Action privilege} in the expected action set
   * on the specified entity id.
   *
   * TODO: remove this once we have api support for OR privilege enforce
   *
   * @param entityId the entity to be checked
   * @param actionSet the set of privileges
   * @param authorizationEnforcer enforcer to make the authorization check
   * @param principal the principal to be checked
   * @throws UnauthorizedException if the principal does not have any privilege in the action set on the entity
   */
  public static void ensureOnePrivilege(co.cask.cdap.proto.id.EntityId entityId, Set<Action> actionSet,
                                        AuthorizationEnforcer authorizationEnforcer,
                                        Principal principal) throws Exception {
    boolean isAuthorized = false;
    for (Action action : actionSet) {
      try {
        authorizationEnforcer.enforce(entityId, principal, action);
        isAuthorized = true;
        break;
      } catch (UnauthorizedException e) {
        // continue to next action
      }
    }
    if (!isAuthorized) {
      throw new UnauthorizedException(principal, actionSet, entityId, false);
    }
  }

  /**
   * Checks the visibility of the entity info in batch size and returns the visible entities
   *
   * @param entityInfo the entity info to check visibility
   * @param authorizationEnforcer enforcer to make the authorization check
   * @param principal the principal to be checked
   * @param transformer the function to transform the entity info to an entity id
   * @param byPassFilter an optional bypass filter which allows to skip the auth check for some entities
   * @return an unmodified list of visible entities
   */
  public static <EntityInfo> List<EntityInfo> isVisible(
    Collection<EntityInfo> entityInfo, AuthorizationEnforcer authorizationEnforcer, Principal principal,
    Function<EntityInfo, EntityId> transformer, @Nullable Predicate<EntityInfo> byPassFilter) throws Exception {
    List<EntityInfo> visibleEntities = new ArrayList<>(entityInfo.size());
    for (List<EntityInfo> split : Iterables.partition(entityInfo,
                                                      Constants.Security.Authorization.VISIBLE_BATCH_SIZE)) {
      Map<EntityId, EntityInfo> datasetTypesMapping = new LinkedHashMap<>(split.size());
      for (EntityInfo info : split) {
        if (byPassFilter != null && byPassFilter.apply(info)) {
          visibleEntities.add(info);
        } else {
          datasetTypesMapping.put(transformer.apply(info), info);
        }
      }
      datasetTypesMapping.keySet().retainAll(authorizationEnforcer.isVisible(datasetTypesMapping.keySet(), principal));
      visibleEntities.addAll(datasetTypesMapping.values());
    }
    return Collections.unmodifiableList(visibleEntities);
  }

  /**
   * Checks if one entity is visible to the principal
   *
   * @param entityId entity id to be checked
   * @param authorizationEnforcer enforcer to make the authorization check
   * @param principal the principal to be checked
   * @throws UnauthorizedException if the principal does not have any privilege in the action set on the entity
   */
  public static void ensureAccess(EntityId entityId, AuthorizationEnforcer authorizationEnforcer,
                                  Principal principal) throws Exception {
    if (authorizationEnforcer.isVisible(Collections.singleton(entityId), principal).isEmpty()) {
      throw new UnauthorizedException(principal, entityId);
    }
  }

  /**
   * Checks if authorization is enabled
   */
  public static boolean isSecurityAuthorizationEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.ENABLED) && cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  /**
   * Helper function, to run the callable as the principal provided and reset back when the call is done
   */
  public static <T> T authorizeAs(String userName, Callable<T> callable) throws Exception {
    String oldUserName = SecurityRequestContext.getUserId();
    SecurityRequestContext.setUserId(userName);
    try {
      return callable.call();
    } finally {
      SecurityRequestContext.setUserId(oldUserName);
    }
  }

  /**
   * Helper function to get the authorizing user for app deployment, the authorzing user will be the app owner if it
   * is present. If not, it will be the namespace owner. If that is also not present, it will be the user who is making
   * the request
   */
  public static String getAppAuthorizingUser(OwnerAdmin ownerAdmin, AuthenticationContext authenticationContext,
                                             ApplicationId applicationId,
                                             @Nullable KerberosPrincipalId appOwner) throws IOException {
    KerberosPrincipalId effectiveOwner =
      SecurityUtil.getEffectiveOwner(ownerAdmin, applicationId.getNamespaceId(),
                                     appOwner == null ? null : appOwner.getPrincipal());
    return effectiveOwner != null ? effectiveOwner.getPrincipal() : authenticationContext.getPrincipal().getName();
  }

  /**
   * Get the effective master user, if it is specified in the {@link CConfiguration}, use it. Otherwise, use the
   * current login user. If security is not enabled, null is returned.
   */
  @Nullable
  public static String getEffectiveMasterUser(CConfiguration cConf) {
    String masterPrincipal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    try {
      if (isSecurityAuthorizationEnabled(cConf)) {
        masterPrincipal = masterPrincipal == null ? UserGroupInformation.getLoginUser().getShortUserName() :
          new KerberosName(masterPrincipal).getShortName();
      } else {
        masterPrincipal = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to translate the principal name %s to an operating system " +
                                                 "user name.", masterPrincipal), e);
    }
    return masterPrincipal;
  }
}
