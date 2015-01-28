/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

/**
 * Provides ways to list and set ACLs.
 */
public class ACLClient {

  private static final Gson GSON = new Gson();

  private final Supplier<URI> baseURI;

  @Inject
  public ACLClient(final DiscoveryServiceClient discoveryServiceClient) {
    this.baseURI = new Supplier<URI>() {
      @Override
      public URI get() {
        ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.ACL);
        Discoverable endpoint = new RandomEndpointStrategy(discovered).pick(5, TimeUnit.SECONDS);
        Preconditions.checkNotNull(endpoint, "No discoverable endpoint found for ACLService");

        InetSocketAddress socketAddress = endpoint.getSocketAddress();
        try {
          // TODO: support https by checking router ssl enabled from Configuration
          String url = String.format("http://%s:%d", socketAddress.getAddress().getHostName(), socketAddress.getPort());
          return new URI(url);
        } catch (URISyntaxException e) {
          return null;
        }
      }
    };
  }

  public List<ACL> listAcls(EntityId entityId) throws IOException {
    URL url = resolveURL(String.format("/v2/admin/acls/%s/%s", entityId.getType().getPluralForm(), entityId.getId()));
    HttpResponse response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, url).build());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ACL>>() { }).getResponseObject();
  }

  public List<ACL> listAcls(EntityId entityId, String userId) throws IOException {
    URL url = resolveURL(String.format("/v2/admin/acls/%s/%s/user/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), userId));
    HttpResponse response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, url).build());
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<ACL>>() { }).getResponseObject();
  }

  public void setAclForUser(EntityId entityId, String userId, List<PermissionType> permissions) throws IOException {
    URL url = resolveURL(String.format("/v2/admin/acls/%s/%s/user/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), userId));
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, url).withBody(GSON.toJson(permissions)).build();
    HttpRequests.execute(request);
  }

  public void setAclForGroup(EntityId entityId, String groupId, List<PermissionType> permissions) throws IOException {
    URL url = resolveURL(String.format("/v2/admin/acls/%s/%s/group/%s", entityId.getType().getPluralForm(),
                                       entityId.getId(), groupId));
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, url).withBody(GSON.toJson(permissions)).build();
    HttpRequests.execute(request);
  }

  private URL resolveURL(String path) throws MalformedURLException {
    return baseURI.get().resolve(path).toURL();
  }
}
