/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.discovery.TimeLimitEndpointStrategy;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.http.ObjectResponse;
import com.google.common.base.Supplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.ws.rs.NotAuthorizedException;

/**
 * Provides ways to list and set ACLs.
 */
public class RemoteACLClient implements ACLClient {

  private static final Gson GSON = new Gson();

  private final Supplier<URI> baseURI;

  @Inject
  public RemoteACLClient(final DiscoveryServiceClient discoveryServiceClient) {
    this.baseURI = new Supplier<URI>() {
      @Override
      public URI get() {
        Iterable<Discoverable> serviceDiscovered = discoveryServiceClient.discover(Constants.Service.ACL);
        TimeLimitEndpointStrategy strategy = new TimeLimitEndpointStrategy(
          new RandomEndpointStrategy(serviceDiscovered), 5, TimeUnit.SECONDS);
        if (strategy.pick() == null) {
          return null;
        }

        InetSocketAddress socketAddress = strategy.pick().getSocketAddress();
        try {
          // TODO: support https by checking router ssl enabled from Configuration
          return new URI("http://" + socketAddress.getHostName() + ":" + socketAddress.getPort());
        } catch (URISyntaxException e) {
          return null;
        }
      }
    };
  }

  @Override
  public List<ACL> listACLs(EntityId entityId) throws IOException {
    URL url = resolveURL(String.format("/admin/acls/%s/%s", entityId.getType().getPluralForm(), entityId.getId()));
    HttpResponse response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, url).build());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ACL>>() { }).getResponseObject();
  }

  @Override
  public List<ACL> listACLs(EntityId entityId, String userId) throws IOException {
    URL url = resolveURL(String.format("/admin/acls/%s/%s/user/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), userId));
    HttpResponse response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, url).build());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ACL>>() { }).getResponseObject();
  }

  @Override
  public void setACLForUser(EntityId entityId, String userId, List<PermissionType> permissions) throws IOException {
    URL url = resolveURL(String.format("/admin/acls/%s/%s/user/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), userId));
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, url).withBody(GSON.toJson(permissions)).build();
    HttpRequests.execute(request);
  }

  @Override
  public void setACLForGroup(EntityId entityId, String groupId, List<PermissionType> permissions) throws IOException {
    URL url = resolveURL(String.format("/admin/acls/%s/%s/group/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), groupId));
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, url).withBody(GSON.toJson(permissions)).build();
    HttpRequests.execute(request);
  }

  private URL resolveURL(String path) throws IOException {
    if (baseURI.get() == null) {
      throw new ConnectException("Could not resolve ACLService");
    }
    return baseURI.get().resolve(Constants.Gateway.GATEWAY_VERSION + "/" + path).toURL();
  }
}
