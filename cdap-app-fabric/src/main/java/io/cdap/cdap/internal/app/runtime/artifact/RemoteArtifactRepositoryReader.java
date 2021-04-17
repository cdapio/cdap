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
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import sun.net.www.protocol.http.HttpURLConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation for fetching artifact metadata from remote {@link ArtifactHttpHandlerInternal}
 */
public class RemoteArtifactRepositoryReader implements ArtifactRepositoryReader {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_DETAIL_TYPE = new TypeToken<ArtifactDetail>() {
  }.getType();
  private static final Type ARTIFACT_DETAIL_LIST_TYPE = new TypeToken<List<ArtifactDetail>>() {
  }.getType();

  private final RemoteClient remoteClient;
  private final LocationFactory locationFactory;

  @Inject
  public RemoteArtifactRepositoryReader(DiscoveryServiceClient discoveryClient,
                                        LocationFactory locationFactory) {
    this.remoteClient = new RemoteClient(
      discoveryClient, Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false), Constants.Gateway.INTERNAL_API_VERSION_3);
    this.locationFactory = locationFactory;
  }

  /**
   * Fetches {@link ArtifactDetail} from {@link AppLifecycleHttpHandler}
   * <p>
   * Note that {@link Location} in {@link ArtifactDescriptor} doesn't get transported over, we need to instantiate
   * it based on the location URI in the received {@link ArtifactDetail} to construct a complete {@link ArtifactDetail}.
   */
  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    HttpResponse httpResponse;
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s",
                               artifactId.getNamespace().getId(),
                               artifactId.getName(),
                               artifactId.getVersion());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());
    ArtifactDetail detail = GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_DETAIL_TYPE);
    Location artifactLocation =
      Locations.getLocationFromAbsolutePath(locationFactory, detail.getDescriptor().getLocationURI().getPath());
    ArtifactDetail artifactDetail =
      new ArtifactDetail(new ArtifactDescriptor(detail.getDescriptor().getArtifactId(), artifactLocation),
                         detail.getMeta());
    return artifactDetail;
  }

  @Override
  public InputStream getArtifactBytes(Id.Artifact artifactId) throws Exception {
    HttpResponse httpResponse;
    String namespaceId = artifactId.getNamespace().getId();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = "default";
      scope = ArtifactScope.SYSTEM;
    }
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                               namespaceId,
                               artifactId.getName(),
                               artifactId.getVersion(),
                               scope);
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());

    return new ByteArrayInputStream(httpResponse.getResponseBody());
  }


  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit, ArtifactSortOrder order)
    throws Exception {
    String url = String.format("namespaces/%s/artifacts/%s/versions?lower=%s&upper=%s&limit=%s&order=%s",
                               range.getNamespace(),
                               range.getName(),
                               range.getLower().toString(),
                               range.getUpper().toString(),
                               String.valueOf(limit),
                               order.name());
    HttpRequest.Builder requestBuilder = remoteClient.requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse = execute(requestBuilder.build());
    List<ArtifactDetail> details = GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_DETAIL_LIST_TYPE);
    List<ArtifactDetail> detailList = new ArrayList<>();
    for (ArtifactDetail detail : details) {
      Location artifactLocation = locationFactory.create(detail.getDescriptor().getLocationURI());
      detailList.add(
        new ArtifactDetail(new ArtifactDescriptor(detail.getDescriptor().getArtifactId(), artifactLocation),
                           detail.getMeta()));
    }
    return detailList;
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
