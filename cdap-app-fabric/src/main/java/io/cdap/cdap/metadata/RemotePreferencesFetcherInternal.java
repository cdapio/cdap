/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Fetch preferences via REST API calls (using internal endpoint {@code INTERNAL_API_VERSION_3})
 */
public class RemotePreferencesFetcherInternal implements PreferencesFetcher {
  private static final Gson GSON = new Gson();
  private static final Type PREFERENCES_TYPE = new TypeToken<PreferencesDetail>() { }.getType();

  private final RemoteClient remoteClient;

  @Inject
  public RemotePreferencesFetcherInternal(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Get preferences for the given identify
   */
  public PreferencesDetail get(EntityId entityId, boolean resolved) throws IOException, NotFoundException {
    HttpResponse httpResponse;
    String url = getPreferencesURI(entityId, resolved);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), PREFERENCES_TYPE);
  }

  /**
   * Construct URI to fetch preferences depending on the type of supplied entity
   */
  private String getPreferencesURI(EntityId entityId, boolean resolved) {
    String uri;
    switch (entityId.getEntityType()) {
      case INSTANCE:
        uri = String.format("preferences");
        break;
      case NAMESPACE:
        NamespaceId namespaceId = (NamespaceId) entityId;
        uri = String.format("namespaces/%s/preferences", namespaceId.getNamespace());
        break;
      case APPLICATION:
        ApplicationId appId = (ApplicationId) entityId;
        uri = String.format("namespaces/%s/apps/%s/preferences",
                            appId.getNamespace(), appId.getApplication());
        break;
      case PROGRAM:
        ProgramId programId = (ProgramId) entityId;
        uri = String.format("namespaces/%s/apps/%s/%s/%s/preferences",
                            programId.getNamespace(), programId.getApplication(), programId.getType().getCategoryName(),
                            programId.getProgram());
        break;
      default:
        throw new UnsupportedOperationException(
          String.format("Preferences cannot be used on this entity type: %s", entityId.getEntityType()));
    }
    if (resolved) {
      uri += "?resolved=true";
    }
    return uri;
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format("Request failed %s", httpResponse.getResponseBodyAsString()));
    }
    return httpResponse;
  }

}
