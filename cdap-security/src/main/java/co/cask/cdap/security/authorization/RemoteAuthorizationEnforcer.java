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
import co.cask.cdap.common.internal.remote.RemoteOpsClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
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
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

/**
 * Remote implementation of the AuthorizationEnforcer. Contacts master for authorization enforcement and
 * then caches the results.
 */
public class RemoteAuthorizationEnforcer extends AbstractAuthorizationEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAuthorizationEnforcer.class);

  private static final int minCacheSize = 12;
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private final RemoteClient remoteClient;
  private final boolean cacheEnabled;

  private final LoadingCache<AuthorizationRequest, Boolean> authPolicyCache;


  @Inject
  public RemoteAuthorizationEnforcer(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    super(cConf);
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
    this.cacheEnabled = cConf.getBoolean(Constants.Security.Authorization.CACHE_ENABLED);
    int cacheTtlSecs = cConf.getInt(Constants.Security.Authorization.CACHE_TTL_SECS);
    int cacheMaxEntries = cConf.getInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES);

    authPolicyCache = CacheBuilder.newBuilder()
      .initialCapacity(minCacheSize)
      .expireAfterWrite(cacheTtlSecs, TimeUnit.SECONDS)
      .maximumSize(cacheMaxEntries)
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

  private boolean doEnforce(AuthorizationRequest authorizationRequest) throws IOException {
    HttpResponse response = executeRequest(authorizationRequest);
    return HttpResponseStatus.OK.getCode() == response.getResponseCode();

  }

  private HttpResponse executeRequest(AuthorizationRequest authorizationRequest) throws IOException {
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.POST, "enforce")
      .withBody(GSON.toJson(authorizationRequest))
      .build();
    return remoteClient.execute(request);
  }
}
