/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.PrivilegesFetcher;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link AuthorizationEnforcementService}.
 */
@Singleton
public class DefaultAuthorizationEnforcementService extends AbstractScheduledService
  implements AuthorizationEnforcementService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizationEnforcementService.class);
  private static final EnumSet<State> SERVICE_AVAILABLE_STATES =
    EnumSet.of(State.STARTING, State.RUNNING, State.STOPPING);
  private static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };

  private final PrivilegesFetcher privilegesFetcher;
  private final boolean authorizationEnabled;
  private final boolean cacheEnabled;
  private final int cacheTtlSecs;
  private final int cacheRefreshIntervalSecs;
  private final LoadingCache<Principal, Map<EntityId, Set<Action>>> authPolicyCache;

  private ScheduledExecutorService executor;

  @Inject
  DefaultAuthorizationEnforcementService(PrivilegesFetcher privilegesFetcher, CConfiguration cConf) {
    this.privilegesFetcher = privilegesFetcher;
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.cacheEnabled = cConf.getBoolean(Constants.Security.Authorization.CACHE_ENABLED);
    this.cacheTtlSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    this.cacheRefreshIntervalSecs = cConf.getInt(Constants.Security.Authorization.CACHE_REFRESH_INTERVAL_SECS);
    validateCacheConfig();
    this.authPolicyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTtlSecs, TimeUnit.SECONDS)
      .build(new CacheLoader<Principal, Map<EntityId, Set<Action>>>() {
        @SuppressWarnings("NullableProblems")
        @Override
        public Map<EntityId, Set<Action>> load(Principal principal) throws Exception {
          return fetchPrivileges(principal);
        }
      });
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, cacheRefreshIntervalSecs, TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting authorization enforcement service...");
    if (isAuthCacheEnabled()) {
      updatePrivilegesOfCurrentUser();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (!isAuthCacheEnabled()) {
      return;
    }
    LOG.trace("Running authorization enforcement service iteration...");
    for (Principal principal : authPolicyCache.asMap().keySet()) {
      updatePrivileges(principal);
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    enforce(entity, principal, Collections.singleton(action));
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    if (!authorizationEnabled) {
      return;
    }
    // For accessing system datasets for internal operations like recording metadata, usage, lineage, etc
    if (Principal.SYSTEM.equals(principal)) {
      return;
    }

    Set<Action> allowedActions = getPrivileges(principal).get(entity);
    LOG.trace("Enforcing actions {} on {} for {}. Allowed actions are {}", actions, entity, principal, allowedActions);
    if (allowedActions == null) {
      throw new UnauthorizedException(principal, actions, entity);
    }

    // If a principal has ALL privileges on the entity, authorization should succeed
    // Otherwise check for the specific actions requested
    if (!allowedActions.contains(Action.ALL) && !allowedActions.containsAll(actions)) {
      throw new UnauthorizedException(principal, Sets.difference(actions, allowedActions), entity);
    }
  }

  @Override
  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
    if (!authorizationEnabled) {
      return ALLOW_ALL;
    }
    // For accessing system datasets for internal operations like recording metadata, usage, lineage, etc
    if (Principal.SYSTEM.equals(principal)) {
      return ALLOW_ALL;
    }
    Map<EntityId, Set<Action>> privileges = getPrivileges(principal);
    final Set<EntityId> allowedEntities = privileges != null ? privileges.keySet() : new HashSet<EntityId>();

    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        return allowedEntities.contains(entityId);
      }
    };
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("authorization-enforcement-service"));
    return executor;
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down authorization enforcement service...");
    authPolicyCache.invalidateAll();
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Shutdown authorization enforcement service successfully.");
  }

  @VisibleForTesting
  Map<Principal, Map<EntityId, Set<Action>>> getCache() {
    return authPolicyCache.asMap();
  }

  private Map<EntityId, Set<Action>> fetchPrivileges(Principal principal) throws Exception {
    State serviceState = state();
    // The only states in which the service can be used are:
    // 1. STARTING - while pre-populating the cache with the current user's privileges
    // 2. RUNNING - from calls to enforce() during program execution, after the service has been started successfully
    // 3. STOPPING - while invalidating the cache during service stop.
    if (!SERVICE_AVAILABLE_STATES.contains(serviceState)) {
      throw new IllegalStateException(
        String.format("Cannot use %s because it has not been started. Its current state is %s.",
                      AuthorizationEnforcementService.class.getName(), serviceState)
      );
    }
    Map<EntityId, Set<Action>> result = new HashMap<>();
    Set<Privilege> privileges = privilegesFetcher.listPrivileges(principal);
    if (privileges == null) {
      return result;
    }

    for (Privilege privilege : privileges) {
      Set<Action> actions = result.get(privilege.getEntity());
      if (actions == null) {
        actions = EnumSet.noneOf(Action.class);
        result.put(privilege.getEntity(), actions);
      }
      actions.add(privilege.getAction());
    }

    return result;
  }

  private Map<EntityId, Set<Action>> getPrivileges(Principal principal) throws Exception {
    return cacheEnabled ? authPolicyCache.get(principal) : fetchPrivileges(principal);
  }
  /**
   * On an authorization-enabled cluster, if caching is enabled too, updates the cache in the
   * {@link AuthorizationEnforcementService} with the privileges of the user running the program.
   */
  private void updatePrivilegesOfCurrentUser() {
    String userName;
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Error while determining the currently logged in user. Skipping pre-population of authorization cache.",
               e);
      return;
    }
    Principal principal = new Principal(userName, Principal.PrincipalType.USER);
    try {
      updatePrivileges(principal);
      LOG.info("Updated privileges for current user {}", principal);
    } catch (Exception e) {
      LOG.warn("Error while updating privileges for {}. Authorization cache will not be pre-populated for this user.",
                principal);
    }
  }

  /**
   * Updates privileges of the specified user in the cache.
   */
  private void updatePrivileges(Principal principal) throws Exception {
    Map<EntityId, Set<Action>> privileges = fetchPrivileges(principal);
    authPolicyCache.put(principal, privileges);
    LOG.debug("Updated privileges for principal {} as {}", principal, privileges);
  }

  private void validateCacheConfig() {
    if (!isAuthCacheEnabled()) {
      if (cacheEnabled) {
        LOG.warn("Authorization policy caching is enabled ({} is set to true), however, this setting will have no " +
                   "effect because authorization is disabled ({} is set to false). ",
                 Constants.Security.Authorization.CACHE_ENABLED, Constants.Security.Authorization.ENABLED);
      }
      return;
    }
    Preconditions.checkArgument(
      cacheRefreshIntervalSecs > 0,
      "The refresh interval for authorization cache specified by the parameter '%s' must be greater than zero. " +
        "It is currently set to %s.",
      Constants.Security.Authorization.CACHE_REFRESH_INTERVAL_SECS, cacheRefreshIntervalSecs);
    Preconditions.checkArgument(
      cacheTtlSecs > 0,
      "The TTL for authorization cache entries specified by the parameter '%s' must be greater than zero. " +
        "It is currently set to %s.",
      Constants.Security.Authorization.CACHE_TTL_SECS, cacheTtlSecs);
    if (cacheTtlSecs <= cacheRefreshIntervalSecs) {
      LOG.warn("The refresh interval for authorization cache specified by the parameter '{}' (set to {}) is " +
                 "greater than the TTL for authorization cache entries specified by the parameter '{}' " +
                 "(set to {}). This may result in authorization errors. Please set the refresh interval to a " +
                 "few seconds less than the TTL to fix this.");
    }
  }

  private boolean isAuthCacheEnabled() {
    return authorizationEnabled && cacheEnabled;
  }
}
