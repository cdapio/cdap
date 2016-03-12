/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.CheckAuthorizedRequest;
import co.cask.cdap.proto.security.GrantRequest;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.RevokeRequest;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Set;
import javax.inject.Inject;

/**
 * Provides ways to interact with the CDAP authorization system.
 */
@Beta
public class AuthorizationClient {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();
  private final RESTClient restClient;
  private final ClientConfig config;

  @Inject
  public AuthorizationClient(ClientConfig config, RESTClient restClient) {
    this.config = config;
    this.restClient = restClient;
  }

  public AuthorizationClient(ClientConfig config) {
    this(config, new RESTClient(config));
  }

  public void authorized(EntityId entity, Principal principal, Action action) throws IOException, UnauthorizedException,
    FeatureDisabledException, co.cask.cdap.security.spi.authorization.UnauthorizedException {

    CheckAuthorizedRequest checkRequest = new CheckAuthorizedRequest(entity, principal, ImmutableSet.of(action));

    URL url = config.resolveURLV3("security/authorized");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(checkRequest)).build();
    executeAuthorizationRequest(request, principal, action, entity);
  }

  public void grant(EntityId entity, Principal principal, Set<Action> actions) throws IOException,
    UnauthorizedException, FeatureDisabledException, co.cask.cdap.security.spi.authorization.UnauthorizedException {

    GrantRequest grantRequest = new GrantRequest(entity, principal, actions);

    URL url = config.resolveURLV3("security/grant");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(grantRequest)).build();
    executeAuthorizationRequest(request, principal, Action.ADMIN, entity);
  }

  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws IOException,
    UnauthorizedException, FeatureDisabledException, co.cask.cdap.security.spi.authorization.UnauthorizedException {
    revoke(new RevokeRequest(entity, principal, actions));
  }

  public void revoke(EntityId entity, Principal principal) throws IOException, UnauthorizedException,
    FeatureDisabledException, co.cask.cdap.security.spi.authorization.UnauthorizedException {
    revoke(new RevokeRequest(entity, principal, null));
  }

  public void revoke(EntityId entity) throws IOException, UnauthorizedException, FeatureDisabledException,
    co.cask.cdap.security.spi.authorization.UnauthorizedException {
    revoke(new RevokeRequest(entity, null, null));
  }

  public void revoke(RevokeRequest revokeRequest) throws IOException, UnauthorizedException, FeatureDisabledException,
    co.cask.cdap.security.spi.authorization.UnauthorizedException {
    URL url = config.resolveURLV3("security/revoke");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(revokeRequest)).build();
    executeAuthorizationRequest(request, revokeRequest.getPrincipal(), Action.ADMIN,
                                revokeRequest.getEntity());
  }

  private void executeAuthorizationRequest(HttpRequest request, Principal principal, Action action, EntityId entity)
    throws IOException, UnauthorizedException, FeatureDisabledException,
    co.cask.cdap.security.spi.authorization.UnauthorizedException {
    HttpResponse response = restClient.execute(request, config.getAccessToken(), HttpURLConnection.HTTP_FORBIDDEN,
                                               HttpURLConnection.HTTP_NOT_IMPLEMENTED);
    if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new co.cask.cdap.security.spi.authorization.UnauthorizedException(principal, action, entity);
    }
    if (HttpURLConnection.HTTP_NOT_IMPLEMENTED == response.getResponseCode()) {
      throw new FeatureDisabledException("Authorization", "cdap-site.xml", Constants.Security.Authorization.ENABLED,
                                         "true");
    }
  }
}
