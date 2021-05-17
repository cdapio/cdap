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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.AuthorizationPrivilege;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.VisibilityRequest;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Remote implementation of the AuthorizationEnforcer. Contacts master for authorization enforcement and
 * then caches the results if caching is enabled.
 */
public class RemoteAccessEnforcer extends AbstractAccessEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAccessEnforcer.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .registerTypeAdapterFactory(new PermissionAdapterFactory())
    .create();
  private static final Type SET_ENTITY_TYPE = new TypeToken<Set<EntityId>>() { }.getType();

  private static final Function<VisibilityKey, EntityId> VISIBILITY_KEY_ENTITY_ID_FUNCTION =
    new Function<VisibilityKey, EntityId>() {
      @Override
      public EntityId apply(VisibilityKey input) {
        return input.getEntityId();
      }
    };

  private static final Predicate<Map.Entry<VisibilityKey, Boolean>> VISIBILITY_KEYS_FILTER =
    new Predicate<Map.Entry<VisibilityKey, Boolean>>() {
      @Override
      public boolean apply(Map.Entry<VisibilityKey, Boolean> input) {
        return input.getValue();
      }
    };

  private final RemoteClient remoteClient;
  private final boolean cacheEnabled;

  private class EnforcementResponse {
    private boolean success;
    private Exception exception;

    EnforcementResponse(boolean success, @Nullable Exception exception) {
      this.success = success;
      this.exception = exception;
    }

    /**
     * Returns whether the enforcement was successful or not.
     * @return Whether authorization succeeded
     */
    public boolean isSuccess() {
      return success;
    }

    /**
     * If the response failed due to some non-authorization reason, this will return the exception.
     * @return The failure exception
     */
    public Exception getException() {
      return exception;
    }
  }
  private final LoadingCache<AuthorizationPrivilege, EnforcementResponse> authPolicyCache;
  private final LoadingCache<AuthorizationPrivilege, EnforcementResponse> singleVisibilityCache;
  private final LoadingCache<VisibilityKey, Boolean> visibilityCache;

  @Inject
  public RemoteAccessEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
    int cacheTTLSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    int cacheMaxEntries = cConf.getInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES);
    // Cache can be disabled by setting the number of entries to <= 0
    this.cacheEnabled = cacheMaxEntries > 0;

    int perCacheSize = cacheMaxEntries / 3 + 1;
    authPolicyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTTLSecs, TimeUnit.SECONDS)
      .maximumSize(perCacheSize)
      .build(new CacheLoader<AuthorizationPrivilege, EnforcementResponse>() {
        @Override
        @ParametersAreNonnullByDefault
        public EnforcementResponse load(AuthorizationPrivilege authorizationPrivilege) throws Exception {
          LOG.trace("Cache miss for {}", authorizationPrivilege);
          return doEnforce(authorizationPrivilege);
        }
      });

    singleVisibilityCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTTLSecs, TimeUnit.SECONDS)
      .maximumSize(perCacheSize)
      .build(new CacheLoader<AuthorizationPrivilege, EnforcementResponse>() {
        @Override
        @ParametersAreNonnullByDefault
        public EnforcementResponse load(AuthorizationPrivilege authorizationPrivilege) throws Exception {
          LOG.trace("Cache miss for {}", authorizationPrivilege);
          return doIsVisibleSingleEntity(authorizationPrivilege);
        }
      });

    visibilityCache = CacheBuilder.newBuilder()
      .expireAfterAccess(cacheTTLSecs, TimeUnit.SECONDS)
      .maximumSize(perCacheSize)
      .build(new CacheLoader<VisibilityKey, Boolean>() {
        @Override
        @ParametersAreNonnullByDefault
        public Boolean load(VisibilityKey key) throws Exception {
          LOG.trace("Cache miss for {}", key);
          return !loadVisibility(Collections.singleton(key)).isEmpty();
        }

        @Override
        public Map<VisibilityKey, Boolean> loadAll(Iterable<? extends VisibilityKey> keys) throws Exception {
          LOG.trace("Cache miss for {}", keys);
          return loadVisibility(keys);
        }
      });
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Permission permission) throws AccessException {
    enforce(entity, principal, Collections.singleton(permission));
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    AuthorizationPrivilege authorizationPrivilege = new AuthorizationPrivilege(principal, entity, permissions);

    try {
      EnforcementResponse res = cacheEnabled ?
        authPolicyCache.get(authorizationPrivilege) : doEnforce(authorizationPrivilege);
      if (!res.isSuccess()) {
        throw res.getException();
      }
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public void isVisible(EntityId entity, Principal principal) throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    AuthorizationPrivilege authorizationPrivilege = new AuthorizationPrivilege(principal, entity,
                                                                               Collections.emptySet());

    try {
      EnforcementResponse res = cacheEnabled ?
        singleVisibilityCache.get(authorizationPrivilege) : doIsVisibleSingleEntity(authorizationPrivilege);
      if (!res.isSuccess()) {
        throw res.getException();
      }
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
    throws AccessException {
    if (!isSecurityAuthorizationEnabled()) {
      return entityIds;
    }

    Preconditions.checkNotNull(entityIds, "entityIds cannot be null");

    try {
      if (cacheEnabled) {
        Iterable<VisibilityKey> visibilityKeys = toVisibilityKeys(principal, entityIds);
        ImmutableMap<VisibilityKey, Boolean> visibilityMap = visibilityCache.getAll(visibilityKeys);
        return toEntityIds(Maps.filterEntries(visibilityMap, VISIBILITY_KEYS_FILTER).keySet());
      } else {
        return visibilityCheckCall(new VisibilityRequest(principal, entityIds));
      }
    } catch (Exception e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @VisibleForTesting
  public void clearCache() {
    authPolicyCache.invalidateAll();
    visibilityCache.invalidateAll();
  }

  private EnforcementResponse doEnforce(AuthorizationPrivilege authorizationPrivilege) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "enforce")
      .withBody(GSON.toJson(authorizationPrivilege))
      .build();
    try {
      HttpResponse response = remoteClient.execute(request);
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return new EnforcementResponse(true, null);
      }
      return new EnforcementResponse(false, new IOException(String.format("Failed to enforce with code %d: %s",
                                                                   response.getResponseCode(),
                                                                   response.getResponseBodyAsString())));
    } catch (UnauthorizedException e) {
      return new EnforcementResponse(false, e);
    }
  }

  private EnforcementResponse doIsVisibleSingleEntity(AuthorizationPrivilege authorizationPrivilege)
    throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "isSingleVisible")
      .withBody(GSON.toJson(authorizationPrivilege))
      .build();
    try {
      HttpResponse response = remoteClient.execute(request);
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return new EnforcementResponse(true, null);
      }
      return new EnforcementResponse(false, new IOException(String.format("Failed to enforce with code %d: %s",
                                                                   response.getResponseCode(),
                                                                   response.getResponseBodyAsString())));
    } catch (UnauthorizedException e) {
      return new EnforcementResponse(false, e);
    }
  }

  private Set<? extends EntityId> visibilityCheckCall(VisibilityRequest visibilityRequest)
    throws IOException, UnauthorizedException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "isVisible")
      .withBody(GSON.toJson(visibilityRequest))
      .build();
    return GSON.fromJson(remoteClient.execute(request).getResponseBodyAsString(), SET_ENTITY_TYPE);
  }

  private Map<VisibilityKey, Boolean> loadVisibility(Iterable<? extends VisibilityKey> keys)
    throws IOException, UnauthorizedException {
    if (!keys.iterator().hasNext()) {
      return Collections.emptyMap();
    }

    // It is okay to use the first principal here, since isVisible request will always come for a single principal
    Principal principal = keys.iterator().next().getPrincipal();
    Set<? extends EntityId> visibleEntities = visibilityCheckCall(new VisibilityRequest(principal, toEntityIds(keys)));

    Map<VisibilityKey, Boolean> keyMap = new HashMap<>();
    for (VisibilityKey key : keys) {
      keyMap.put(key, visibleEntities.contains(key.getEntityId()));
    }
    return keyMap;
  }

  private Set<? extends EntityId> toEntityIds(Iterable<? extends VisibilityKey> keys) {
    return ImmutableSet.copyOf(Iterables.transform(keys, VISIBILITY_KEY_ENTITY_ID_FUNCTION));
  }

  private Iterable<VisibilityKey> toVisibilityKeys(final Principal principal, Set<? extends EntityId> entityIds) {
    return Iterables.transform(entityIds, new Function<EntityId, VisibilityKey>() {
      @Override
      public VisibilityKey apply(EntityId entityId) {
        return new VisibilityKey(principal, entityId);
      }
    });
  }

  private static class VisibilityKey {
    private final Principal principal;
    private final EntityId entityId;

    VisibilityKey(Principal principal, EntityId entityId) {
      this.principal = principal;
      this.entityId = entityId;
    }

    public Principal getPrincipal() {
      return principal;
    }

    public EntityId getEntityId() {
      return entityId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      VisibilityKey that = (VisibilityKey) o;
      return Objects.equals(principal, that.principal) &&
        Objects.equals(entityId, that.entityId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(principal, entityId);
    }

    @Override
    public String toString() {
      return "VisibilityKey {" +
        "principal=" + principal +
        ", entityId=" + entityId +
        '}';
    }
  }
}
