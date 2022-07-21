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
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.cdap.common.http.ObjectResponse;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fetch application detail via internal REST API calls
 */
public class RemoteApplicationDetailFetcher implements ApplicationDetailFetcher {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Type APPLICATION_DETAIL_LIST_TYPE = new TypeToken<List<ApplicationDetail>>() { }.getType();
  private static final Type JSON_OBJECT_TYPE = new TypeToken<JsonObject>() { }.getType();

  private static final String APPLICATIONS_KEY = "applications";
  private static final String NEXT_PAGE_TOKEN_KEY = "nextPageToken";

  private final RemoteClient remoteClient;

  @Inject
  public RemoteApplicationDetailFetcher(RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false),
      Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Get the application detail for the given application id
   */
  public ApplicationDetail get(ApplicationId appId)
    throws IOException, NotFoundException, UnauthorizedException {
    String url = String.format("namespaces/%s/app/%s/versions/%s",
                               appId.getNamespace(), appId.getApplication(), appId.getVersion());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse;
    httpResponse = execute(requestBuilder.build());
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), ApplicationDetail.class);
  }

  /**
   * Scans all application details in the given namespace
   */
  @Override
  public void scan(String namespace, Consumer<ApplicationDetail> consumer, Integer batchSize)
    throws IOException, NamespaceNotFoundException {
    String url = String.format("namespaces/%s/apps?pageSize=%s", namespace, batchSize);
    String token;

    do {
      HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
      HttpResponse httpResponse;
      try {
        httpResponse = execute(requestBuilder.build());
      } catch (NotFoundException e) {
        throw new NamespaceNotFoundException(new NamespaceId(namespace));
      }
      ObjectResponse<JsonObject> objectResponse =
          ObjectResponse.fromJsonBody(httpResponse, JSON_OBJECT_TYPE, GSON);

      List<ApplicationDetail> appDetails = GSON.fromJson(
          objectResponse.getResponseObject().getAsJsonArray(APPLICATIONS_KEY),
          APPLICATION_DETAIL_LIST_TYPE);

      appDetails.forEach(d -> consumer.accept(d));

      if (objectResponse.getResponseObject().has(NEXT_PAGE_TOKEN_KEY)) {
        token = objectResponse.getResponseObject().getAsJsonPrimitive(NEXT_PAGE_TOKEN_KEY).getAsString();
      } else {
        token = null;
      }

      url = String.format("namespaces/%s/apps?pageSize=%s&pageToken=%s", namespace, batchSize, token);

    } while (token != null);
}

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException, UnauthorizedException {
    HttpResponse httpResponse = remoteClient.execute(request);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }
}
