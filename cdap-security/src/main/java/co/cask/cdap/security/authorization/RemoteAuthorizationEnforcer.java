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
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.internal.remote.MethodArgument;
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Remote implementation of the AuthorizationEnforcer. Contacts master for authorization enforcement and
 * then caches the results if caching is enabled.
 */
public class RemoteAuthorizationEnforcer extends AbstractAuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAuthorizationEnforcer.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private final RemoteClient remoteClient;
  private final boolean cacheEnabled;

  private static LoadingCache<AuthorizationPrivilege, Boolean> authPolicyCache;

  @Inject
  public RemoteAuthorizationEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
    this.cacheEnabled = cConf.getBoolean(Constants.Security.Authorization.CACHE_ENABLED);
    int cacheTtlSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    int cacheMaxEntries = cConf.getInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES);

    if (cacheEnabled) {
      LOG.debug("Initializing authorization cache for containers with TTL {} and max entries as {}", cacheTtlSecs,
                cacheMaxEntries);
      authPolicyCache = CacheBuilder.newBuilder()
        .expireAfterWrite(cacheTtlSecs, TimeUnit.SECONDS)
        .maximumSize(cacheMaxEntries)
        .build(new CacheLoader<AuthorizationPrivilege, Boolean>() {
          @SuppressWarnings("NullableProblems")
          @Override
          public Boolean load(AuthorizationPrivilege authorizationPrivilege) throws Exception {
            LOG.trace("Cache miss for " + authorizationPrivilege);
            return doEnforce(authorizationPrivilege);
          }
        });
    } else {
      authPolicyCache = null;
    }
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    LOG.debug("Enforcing {} on {} for {}", action, entity, principal);
    AuthorizationPrivilege authorizationPrivilege = new AuthorizationPrivilege(principal, entity, action);

    boolean allowed = cacheEnabled ? authPolicyCache.get(authorizationPrivilege) : doEnforce(authorizationPrivilege);
    if (!allowed) {
      throw new UnauthorizedException(principal, action, entity);
    }
  }

  private boolean doEnforce(AuthorizationPrivilege authorizationPrivilege) throws IOException {
    HttpResponse response = executeRequest(authorizationPrivilege);
    return HttpResponseStatus.OK.getCode() == response.getResponseCode();
  }

  private HttpResponse executeRequest(AuthorizationPrivilege authorizationPrivilege) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "enforce")
      .withBody(GSON.toJson(createBody(authorizationPrivilege.getEntityId(),
                                       authorizationPrivilege.getPrincipal(),
                                       authorizationPrivilege.getAction())))
      .build();
    return remoteClient.execute(request);
  }

  private static List<MethodArgument> createBody(Object... arguments) {
    List<MethodArgument> methodArguments = new ArrayList<>();
    for (Object arg : arguments) {
      if (arg == null) {
        methodArguments.add(null);
      } else {
        String type = arg.getClass().getName();
        methodArguments.add(new MethodArgument(type, GSON.toJsonTree(arg)));
      }
    }
    return methodArguments;
  }

  @VisibleForTesting
  public Map<AuthorizationPrivilege, Boolean> cacheAsMap() {
    return Collections.unmodifiableMap(authPolicyCache.asMap());
  }

  /**
   * Key for caching Privileges on containers. This represents a specific privilege on which authorization can be
   * enforced. The cache stores whether the enforce succeeded or failed.
   */
  public static class AuthorizationPrivilege {

    private final Principal principal;
    private final EntityId entityId;
    private final Action action;

    public AuthorizationPrivilege(Principal principal, EntityId entityId, Action action) {
      this.principal = principal;
      this.entityId = entityId;
      this.action = action;
    }

    public Principal getPrincipal() {
      return principal;
    }

    public EntityId getEntityId() {
      return entityId;
    }

    public Action getAction() {
      return action;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AuthorizationPrivilege that = (AuthorizationPrivilege) o;
      return Objects.equals(principal, that.principal) &&
        Objects.equals(entityId, that.entityId) &&
        action == that.action;
    }

    @Override
    public int hashCode() {
      return Objects.hash(principal, entityId, action);
    }

    @Override
    public String toString() {
      return "AuthorizationPrivilege{" +
        "principal=" + principal +
        ", entityId=" + entityId +
        ", action=" + action +
        '}';
    }
  }
}
