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
import co.cask.cdap.common.internal.remote.RemoteClient;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.AuthorizationPrivilege;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.VisibilityRequest;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
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
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Remote implementation of the AuthorizationEnforcer. Contacts master for authorization enforcement and
 * then caches the results if caching is enabled.
 */
public class RemoteAuthorizationEnforcer extends AbstractAuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAuthorizationEnforcer.class);

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
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

  private final LoadingCache<AuthorizationPrivilege, Boolean> authPolicyCache;
  private final LoadingCache<VisibilityKey, Boolean> visibilityCache;

  @Inject
  public RemoteAuthorizationEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
    int cacheTTLSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    int cacheMaxEntries = cConf.getInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES);
    // Cache can be disabled by setting the number of entries to <= 0
    this.cacheEnabled = cacheMaxEntries > 0;

    int perCacheSize = cacheMaxEntries / 2 + 1;
    authPolicyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTTLSecs, TimeUnit.SECONDS)
      .maximumSize(perCacheSize)
      .build(new CacheLoader<AuthorizationPrivilege, Boolean>() {
        @Override
        @ParametersAreNonnullByDefault
        public Boolean load(AuthorizationPrivilege authorizationPrivilege) throws Exception {
          LOG.trace("Cache miss for {}", authorizationPrivilege);
          return doEnforce(authorizationPrivilege);
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
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return;
    }
    AuthorizationPrivilege authorizationPrivilege = new AuthorizationPrivilege(principal, entity, action);

    boolean allowed = cacheEnabled ? authPolicyCache.get(authorizationPrivilege) : doEnforce(authorizationPrivilege);
    if (!allowed) {
      throw new UnauthorizedException(principal, action, entity);
    }
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) throws Exception {
    if (!isSecurityAuthorizationEnabled()) {
      return entityIds;
    }

    Preconditions.checkNotNull(entityIds, "entityIds cannot be null");

    if (cacheEnabled) {
      Iterable<VisibilityKey> visibilityKeys = toVisibilityKeys(principal, entityIds);
      ImmutableMap<VisibilityKey, Boolean> visibilityMap = visibilityCache.getAll(visibilityKeys);
      return toEntityIds(Maps.filterEntries(visibilityMap, VISIBILITY_KEYS_FILTER).keySet());
    } else {
      return visibilityCheckCall(new VisibilityRequest(principal, entityIds));
    }
  }

  @VisibleForTesting
  public void clearCache() {
    authPolicyCache.invalidateAll();
    visibilityCache.invalidateAll();
  }

  private boolean doEnforce(AuthorizationPrivilege authorizationPrivilege) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "enforce")
      .withBody(GSON.toJson(authorizationPrivilege))
      .build();
    return HttpURLConnection.HTTP_OK == remoteClient.execute(request).getResponseCode();
  }

  private Set<? extends EntityId> visibilityCheckCall(VisibilityRequest visibilityRequest) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "isVisible")
      .withBody(GSON.toJson(visibilityRequest))
      .build();
    return GSON.fromJson(remoteClient.execute(request).getResponseBodyAsString(), SET_ENTITY_TYPE);
  }

  private Map<VisibilityKey, Boolean> loadVisibility(Iterable<? extends VisibilityKey> keys) throws IOException {
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
      return "VisibilityKey{" +
        "principal=" + principal +
        ", entityId=" + entityId +
        '}';
    }
  }
}
