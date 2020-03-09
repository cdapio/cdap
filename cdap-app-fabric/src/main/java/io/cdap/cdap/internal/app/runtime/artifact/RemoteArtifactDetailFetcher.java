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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.IOException;
import java.lang.reflect.Type;


/**
 * Interface for fetching {@code PreferencesDetail}
 */
public class RemoteArtifactDetailFetcher implements ArtifactDetailFetcher {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_DETAIL_TYPE = new TypeToken<ArtifactDetail>() { }.getType();

  private final RemoteClient remoteClient;


  @Inject
  public RemoteArtifactDetailFetcher(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  @Override
  public ArtifactDetail get(ArtifactId artifactId) throws Exception {
    HttpResponse httpResponse;
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s",
                               artifactId.getNamespace(),
                               artifactId.getArtifact(),
                               artifactId.getVersion());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());
    return GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_DETAIL_TYPE);
  }

  private HttpResponse execute(HttpRequest request) throws IOException, NotFoundException {
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
