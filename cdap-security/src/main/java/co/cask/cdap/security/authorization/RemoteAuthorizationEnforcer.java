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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.internal.remote.RemoteOpsClient;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpResponse;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Remote implementation of the AuthorizationEnforcer.
 */
public class RemoteAuthorizationEnforcer extends RemoteOpsClient implements AuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAuthorizationEnforcer.class);
  private static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };

  private final LoadingCache<AuthorizationRequest, Boolean> authPolicyCache;
  private final boolean cacheEnabled;
  private final boolean securityEnabled;
  private final boolean authorizationEnabled;

  @Inject
  public RemoteAuthorizationEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(discoveryClient, Constants.Service.REMOTE_SYSTEM_OPERATION);
    this.securityEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    cacheEnabled = cConf.getBoolean(Constants.Security.Authorization.CACHE_ENABLED);
    int cacheTtlSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);

    authPolicyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTtlSecs, TimeUnit.SECONDS)
      .build(new CacheLoader<AuthorizationRequest, Boolean>() {
        @SuppressWarnings("NullableProblems")
        @Override
        public Boolean load(AuthorizationRequest authorizationRequest) throws Exception {
          return doEnforce(authorizationRequest);
        }
      });
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    AuthorizationRequest authorizationRequest = new AuthorizationRequest(principal, entity, action);

    boolean allowed = cacheEnabled ? authPolicyCache.get(authorizationRequest) : doEnforce(authorizationRequest);
    if (!allowed) {
      throw new UnauthorizedException(principal, action, entity);
    }
  }

  /**
   * We don't need to handle privilege propagation here because it is already handled in the master.
   *
   * If this privilege is not in the cache then the call will go to the master which will handle privilege propagation
   * and return the value for this privilege accordingly.
   *
   * @param entity the {@link EntityId} on which authorization is to be enforced
   * @param principal the {@link Principal} that performs the actions
   * @param actions the {@link Action actions} being performed
   * @throws Exception
   */
  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    Set<Action> disallowed = new HashSet<>(actions.size());
    for (Action action : actions) {
      try {
        enforce(entity, principal, action);
      } catch (Exception ex) {
        disallowed.add(action);
      }
    }
    if (disallowed.size() > 0) {
      throw new UnauthorizedException(principal, disallowed, entity);
    }
  }

  @Override
  public Predicate<EntityId> createFilter(final Principal principal) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return ALLOW_ALL;
    }
    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        try {
          enforce(entityId, principal, new HashSet<>(Arrays.asList(Action.values())));
          return true;
        } catch (Exception e) {
          return false;
        }
      }
    };
  }

  private boolean doEnforce(AuthorizationRequest authorizationRequest) {
    HttpResponse response = executeRequest("enforce", authorizationRequest.getEntityId(),
                                             authorizationRequest.getPrincipal(),
                                            authorizationRequest.getAction());
    return HttpResponseStatus.OK.getCode() == response.getResponseCode();

  }

  private boolean isSecurityAuthorizationEnabled() {
    return securityEnabled && authorizationEnabled;
  }
}
