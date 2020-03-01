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
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;

/**
 * Fetch application detail via REST API calls
 */
public class RemoteApplicationDetailFetcher implements ApplicationDetailFetcher {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();

  private final RemoteClient remoteClient;

  @Inject
  public RemoteApplicationDetailFetcher(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient,
                                         Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         Constants.Gateway.API_VERSION_3);
  }

  /**
   * Get the application detail for the given application id
   */
  public ApplicationDetail get(ApplicationId appId) throws IOException, NotFoundException {
    String url = String.format("namespaces/%s/appdetail/%s/versions/%s",
                               appId.getNamespace(), appId.getApplication(), appId.getVersion());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    httpResponse = execute(requestBuilder.build());
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), ApplicationDetail.class);
  }

  /**
   * Get details of all applications in the given namespace
   */
  public List<ApplicationDetail> list(String namespace) throws IOException, NamespaceNotFoundException {
    String url = String.format("namespaces/%s/appdetail", namespace);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    try {
      httpResponse = execute(requestBuilder.build());
    } catch (NotFoundException e) {
      throw new NamespaceNotFoundException(new NamespaceId(namespace));
    }
    ObjectResponse<List<ApplicationDetail>> objectResponse = ObjectResponse.fromJsonBody(
      httpResponse, new TypeToken<List<ApplicationDetail>>() { }.getType(), GSON);
    return objectResponse.getResponseObject();
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
