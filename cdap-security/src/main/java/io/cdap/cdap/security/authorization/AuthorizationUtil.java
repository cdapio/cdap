/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationResponse;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for Authorization.
 */
public final class AuthorizationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationUtil.class);

  private AuthorizationUtil() {
  }

  /**
   * Checks the visibility of the entity info in batch size and returns the visible entities.
   *
   * @param entityInfo the entity info to check visibility
   * @param accessEnforcer enforcer to make the authorization check
   * @param principal the principal to be checked
   * @param transformer the function to transform the entity info to an entity id
   * @param byPassFilter an optional bypass filter which allows to skip the auth check for some entities
   * @return an unmodified list of visible entities
   */
  public static <EntityInfo> List<EntityInfo> isVisible(
      Collection<EntityInfo> entityInfo, AccessEnforcer accessEnforcer, Principal principal,
      Function<EntityInfo, EntityId> transformer, @Nullable Predicate<EntityInfo> byPassFilter)
      throws Exception {
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
      datasetTypesMapping.keySet()
          .retainAll(accessEnforcer.isVisible(datasetTypesMapping.keySet(), principal));
      visibleEntities.addAll(datasetTypesMapping.values());
    }
    return Collections.unmodifiableList(visibleEntities);
  }

  /**
   * Checks if authorization is enabled.
   */
  public static boolean isSecurityAuthorizationEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.ENABLED) && cConf.getBoolean(
        Constants.Security.Authorization.ENABLED);
  }

  /**
   * Helper function, to run the callable as the principal provided and reset back when the call is
   * done.
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
   * Helper function to get the authorizing user for app deployment, the authorzing user will be the
   * app owner if it is present. If not, it will be the namespace owner. If that is also not
   * present, it will be the user who is making the request
   */
  public static String getAppAuthorizingUser(OwnerAdmin ownerAdmin,
      AuthenticationContext authenticationContext,
      ApplicationId applicationId,
      @Nullable KerberosPrincipalId appOwner) throws IOException {
    KerberosPrincipalId effectiveOwner =
        SecurityUtil.getEffectiveOwner(ownerAdmin, applicationId.getNamespaceId(),
            appOwner == null ? null : appOwner.getPrincipal());

    // CDAP-13154 If impersonation is configured for either the application or namespace the effective owner will be
    // a kerberos principal which can have different form
    // (refer: https://docs.oracle.com/cd/E21455_01/common/tutorials/kerberos_principal.html). For example it can be
    // a complete principal name (alice/somehost.net@someREALM). For authorization we need the enforcement to happen
    // on the username and not the complete principal. The user name is the shortname of the principal so return the
    // shortname as authorizing user.
    String appAuthorizingUser = effectiveOwner != null
        ? new KerberosName(effectiveOwner.getPrincipal()).getShortName()
        : authenticationContext.getPrincipal().getName();
    LOG.trace("Returning {} as authorizing app user for {}", appAuthorizingUser, applicationId);
    return appAuthorizingUser;
  }

  /**
   * Get the effective master user, if it is specified in the {@link CConfiguration}, use it.
   * Otherwise, use the current login user. If security is not enabled, null is returned.
   */
  @Nullable
  public static String getEffectiveMasterUser(CConfiguration cConf) {
    String masterPrincipal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    try {
      if (isSecurityAuthorizationEnabled(cConf)) {
        masterPrincipal =
            masterPrincipal == null ? UserGroupInformation.getLoginUser().getShortUserName() :
                new KerberosName(masterPrincipal).getShortName();
      } else {
        masterPrincipal = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to translate the principal name %s to an operating system "
              + "user name.", masterPrincipal), e);
    }
    return masterPrincipal;
  }

  /**
   * This method store data in {@link SecurityRequestContext}.
   */
  public static void setAuthorizationDataInContext(AuthorizationResponse authorizationResponse) {
    SecurityRequestContext.enqueueAuditLogContext(authorizationResponse.getAuditLogContext());
  }

  public static void setAuthorizationDataInContext(Collection<AuthorizationResponse> authorizationResponseList) {
    authorizationResponseList.stream().forEach(x -> AuthorizationUtil.setAuthorizationDataInContext(x));
  }

  /**
   * Throw a {@link UnauthorizedException} if the {@link AuthorizationResponse} is having
   * "UNAUTHORIZED" state.
   */
  public static void throwIfUnauthorized(AuthorizationResponse authorizationResponse) {
    if (authorizationResponse.isAuthorized() == AuthorizationResponse.AuthorizationStatus.UNAUTHORIZED) {
      throw createUnauthorizedException(authorizationResponse);
    }
  }

  /**
   * Creates a {@link UnauthorizedException} from a {@link AuthorizationResponse}.
   *
   * @return {@link UnauthorizedException}
   */
  public static UnauthorizedException createUnauthorizedException(AuthorizationResponse authorizationResponse) {
    return new UnauthorizedException(authorizationResponse.getPrincipal(),
                                     authorizationResponse.getMissingPermissions(),
                                     authorizationResponse.getEntity(),
                                     null,
                                     authorizationResponse.isRequiresAllPermissions(),
                                     authorizationResponse.isIncludePrincipal(),
                                     authorizationResponse.getAddendum());
  }

  static void incrementCheckMetricExtension(MetricsContext metricsContext,
                                            AuthorizationResponse authorizationResponse) {
    switch (authorizationResponse.isAuthorized()) {
      case AUTHORIZED:
        metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
        break;
      case UNAUTHORIZED:
        metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_UNAUTHORIZED_COUNT, 1);
        break;
      case NOT_REQUIRED:
        metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
        break;

      default:
        metricsContext.increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
    }
  }
}
