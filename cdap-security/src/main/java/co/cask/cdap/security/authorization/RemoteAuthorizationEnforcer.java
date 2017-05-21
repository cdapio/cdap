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
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.Map;
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

  private final RemoteClient remoteClient;
  private final boolean cacheEnabled;

  private final LoadingCache<AuthorizationPrivilege, Boolean> authPolicyCache;

  @Inject
  public RemoteAuthorizationEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
    int cacheTTLSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    int cacheMaxEntries = cConf.getInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES);
    // Cache can be disabled by setting the number of entries to <= 0
    this.cacheEnabled = cacheMaxEntries > 0;

    authPolicyCache = CacheBuilder.newBuilder()
      .expireAfterWrite(cacheTTLSecs, TimeUnit.SECONDS)
      .maximumSize(cacheMaxEntries)
      .build(new CacheLoader<AuthorizationPrivilege, Boolean>() {
        @Override
        @ParametersAreNonnullByDefault
        public Boolean load(AuthorizationPrivilege authorizationPrivilege) throws Exception {
          LOG.trace("Cache miss for {}", authorizationPrivilege);
          return doEnforce(authorizationPrivilege);
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

  private boolean doEnforce(AuthorizationPrivilege authorizationPrivilege) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "enforce")
      .withBody(GSON.toJson(authorizationPrivilege))
      .build();
    return HttpURLConnection.HTTP_OK == remoteClient.execute(request).getResponseCode();
  }

  @VisibleForTesting
  public Map<AuthorizationPrivilege, Boolean> cacheAsMap() {
    return Collections.unmodifiableMap(authPolicyCache.asMap());
  }
}
