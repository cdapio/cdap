/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.FeatureDisabledException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantRequest;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.RevokeRequest;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.AccessIOException;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AlreadyExistsException;
import io.cdap.cdap.security.spi.authorization.NotFoundException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Provides ways to interact with the CDAP authorization system.
 */
@Beta
public class AuthorizationClient implements AccessController {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .registerTypeAdapterFactory(new PermissionAdapterFactory())
      .create();
  public static final String AUTHORIZATION_BASE = "security/authorization/";
  private static final TypeToken<Set<GrantedPermission>> TYPE_OF_PRIVILEGE_SET =
      new TypeToken<Set<GrantedPermission>>() {
      };
  private static final TypeToken<Set<Role>> TYPE_OF_ROLE_SET = new TypeToken<Set<Role>>() {
  };
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

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
    throw new UnsupportedOperationException(
        "Enforcement is not supported via Java Client. Please instead use the "
            + "listPrivileges method to view the privileges for a principal.");
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
      Permission permission) {
    throw new UnsupportedOperationException(
        "Enforcement is not supported via Java Client. Please instead use the "
            + "listPrivileges method to view the privileges for a principal.");
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
    throw new UnsupportedOperationException("Visibility check is not supported via Java Client.");
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal,
      Set<? extends Permission> permissions)
      throws AccessException {
    GrantRequest grantRequest = new GrantRequest(authorizable, principal, permissions);

    URL url = resolveUrl(AUTHORIZATION_BASE + "/privileges/grant");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(grantRequest)).build();
    executePrivilegeRequest(request);
  }

  @Override
  public void revoke(Authorizable authorizable) throws AccessException {
    revoke(authorizable, null, null);
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal,
      Set<? extends Permission> permissions)
      throws AccessException {
    revoke(new RevokeRequest(authorizable, principal, permissions));
  }

  private void revoke(RevokeRequest revokeRequest) throws AccessException {
    URL url = resolveUrl(AUTHORIZATION_BASE + "/privileges/revoke");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(revokeRequest)).build();
    executePrivilegeRequest(request);
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    String urlStr = String.format(AUTHORIZATION_BASE + "%s/%s/privileges", principal.getType(),
        principal.getName());
    URL url = resolveUrl(urlStr);
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = doExecuteRequest(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, TYPE_OF_PRIVILEGE_SET, GSON).getResponseObject();
    }
    throw new AccessIOException(String.format("Cannot list privileges. Reason: %s",
        response.getResponseBodyAsString()));
  }

  @Override
  public void createRole(Role role) throws AccessException {
    URL url = resolveUrl(String.format(AUTHORIZATION_BASE + "roles/%s", role.getName()));
    HttpRequest request = HttpRequest.put(url).build();
    HttpResponse httpResponse = doExecuteRequest(request, HttpURLConnection.HTTP_CONFLICT);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new AlreadyExistsException(role);
    }
  }

  @Override
  public void dropRole(Role role) throws AccessException {
    URL url = resolveUrl(String.format(AUTHORIZATION_BASE + "roles/%s", role.getName()));
    HttpRequest request = HttpRequest.delete(url).build();
    executeExistingRolesRequest(role, request);
  }

  @Override
  public Set<Role> listAllRoles() throws AccessException {
    return listRolesHelper(null);
  }

  @Override
  public Set<Role> listRoles(Principal principal) throws FeatureDisabledException, AccessException {
    return listRolesHelper(principal);
  }

  @Override
  public void addRoleToPrincipal(Role role, Principal principal) throws AccessException {
    URL url = resolveUrl(String.format(AUTHORIZATION_BASE + "%s/%s/roles/%s", principal.getType(),
                                       principal.getName(), role.getName()));
    HttpRequest request = HttpRequest.put(url).build();
    executeExistingRolesRequest(role, request);
  }

  @Override
  public void removeRoleFromPrincipal(Role role, Principal principal) throws AccessException {
    URL url = resolveUrl(String.format(AUTHORIZATION_BASE + "%s/%s/roles/%s", principal.getType(),
                                       principal.getName(), role.getName()));
    HttpRequest request = HttpRequest.delete(url).build();
    executeExistingRolesRequest(role, request);
  }

  private Set<Role> listRolesHelper(@Nullable Principal principal) throws AccessException {
    URL url = principal == null ? resolveUrl(AUTHORIZATION_BASE + "roles") :
        resolveUrl(String.format(AUTHORIZATION_BASE + "%s/%s/roles", principal.getType(),
                                 principal.getName()));
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = doExecuteRequest(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, TYPE_OF_ROLE_SET).getResponseObject();
    }
    throw new AccessIOException(
        String.format("Cannot list roles. Reason: %s", response.getResponseBodyAsString()));
  }

  private void executeExistingRolesRequest(Role role, HttpRequest request) throws AccessException {
    HttpResponse httpResponse = doExecuteRequest(request, HttpURLConnection.HTTP_NOT_FOUND);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(role);
    }
  }

  private HttpResponse executePrivilegeRequest(HttpRequest request) throws AccessException {
    HttpResponse httpResponse = doExecuteRequest(request, HttpURLConnection.HTTP_NOT_FOUND);
    if (HttpURLConnection.HTTP_NOT_FOUND == httpResponse.getResponseCode()) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }

  private HttpResponse doExecuteRequest(HttpRequest request, int... additionalAllowedErrorCodes)
      throws AccessException {
    try {
      int[] allowedErrorCodes = new int[additionalAllowedErrorCodes.length + 2];
      System.arraycopy(additionalAllowedErrorCodes, 0, allowedErrorCodes, 0,
          additionalAllowedErrorCodes.length);
      allowedErrorCodes[additionalAllowedErrorCodes.length] = HttpURLConnection.HTTP_NOT_IMPLEMENTED;
      HttpResponse response = restClient.execute(request, config.getAccessToken(),
          allowedErrorCodes);
      if (HttpURLConnection.HTTP_NOT_IMPLEMENTED == response.getResponseCode()) {
        FeatureDisabledException.Feature feature = FeatureDisabledException.Feature.AUTHORIZATION;
        String enableConfig = Constants.Security.Authorization.ENABLED;
        if (response.getResponseBodyAsString().toLowerCase().contains("authentication")) {
          feature = FeatureDisabledException.Feature.AUTHENTICATION;
          enableConfig = Constants.Security.ENABLED;
        }
        throw new FeatureDisabledException(feature, FeatureDisabledException.CDAP_SITE,
            enableConfig, "true");
      }
      return response;
    } catch (IOException e) {
      throw new AccessIOException(e);
    }
  }

  private URL resolveUrl(String urlStr) throws AccessIOException {
    try {
      return config.resolveURLV3(urlStr);
    } catch (MalformedURLException e) {
      throw new AccessIOException(e);
    }
  }
}
