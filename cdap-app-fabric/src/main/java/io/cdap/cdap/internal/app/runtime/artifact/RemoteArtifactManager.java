/*
 * Copyright © 2017-2018 Cask Data, Inc.
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


import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.worker.sidecar.ArtifactLocalizerClient;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Implementation for {@link io.cdap.cdap.api.artifact.ArtifactManager}
 * communicating with internal REST endpoints {@link io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal}
 * and returning artifact info.
 */
public final class RemoteArtifactManager extends AbstractArtifactManager {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();

  private final LocationFactory locationFactory;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;
  private final RemoteClient remoteClientInternal;
  private final ArtifactLocalizerClient artifactLocalizerClient;

  @Inject
  RemoteArtifactManager(CConfiguration cConf, LocationFactory locationFactory, @Assisted NamespaceId namespaceId,
                        @Assisted RetryStrategy retryStrategy, RemoteClientFactory remoteClientFactory,
                        Optional<ArtifactLocalizerClient> optionalArtifactLocalizerClient) {
    super(cConf);
    this.locationFactory = locationFactory;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
    this.artifactLocalizerClient = optionalArtifactLocalizerClient.orElse(null);
    this.remoteClientInternal = remoteClientFactory.createRemoteClient(
      Constants.Service.APP_FABRIC_HTTP,
      new DefaultHttpRequestConfig(false),
      String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace and also the artifacts in system
   * namespace.
   * <p>
   * If the app-fabric service is unavailable, it will be retried based on the passed in retry strategy.
   *
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  @Override
  public List<ArtifactInfo> listArtifacts() throws IOException, UnauthorizedException {
    return listArtifacts(namespaceId.getNamespace());
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace and also the artifacts in system
   * namespace.
   * <p>
   * If the app-fabric service is unavailable, it will be retried based on the passed in retry strategy.
   *
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  @Override
  public List<ArtifactInfo> listArtifacts(String namespace) throws IOException, UnauthorizedException {
    try {
      return Retries.callWithRetries(() -> {
        HttpRequest.Builder requestBuilder =
          remoteClientInternal.requestBuilder(HttpMethod.GET,
                                              String.format("namespaces/%s/artifacts", namespace));
        return getArtifactsList(requestBuilder.build());
      }, retryStrategy);
    } catch (UnauthorizedException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private List<ArtifactInfo> getArtifactsList(HttpRequest httpRequest) throws IOException, UnauthorizedException {
    HttpResponse httpResponse = remoteClientInternal.execute(httpRequest);

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException("Could not list artifacts, endpoint not found");
    }

    if (httpResponse.getResponseCode() == 200) {
      return GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_INFO_LIST_TYPE);
    }

    throw new IOException("Exception while getting artifacts list " + httpResponse.getResponseBodyAsString());
  }

  @Override
  protected Location getArtifactLocation(ArtifactInfo artifactInfo,
                                         @Nullable String artifactNamespace) throws IOException, UnauthorizedException {

    String namespace;
    if (ArtifactScope.SYSTEM.equals(artifactInfo.getScope())) {
      namespace = NamespaceId.SYSTEM.getNamespace();
    } else if (artifactNamespace != null) {
      namespace = artifactNamespace;
    } else {
      namespace = namespaceId.getNamespace();
    }

    // If ArtifactLocalizerClient is set, use it to get the cached location of artifact.
    if (artifactLocalizerClient != null) {
      ArtifactId artifactId = new ArtifactId(namespace, artifactInfo.getName(), artifactInfo.getVersion());
      try {
        // Always request for the unpacked version since the artifaction location is used for creation of
        // artifact ClassLoader only.
        return Locations.toLocation(artifactLocalizerClient.getUnpackedArtifactLocation(artifactId));
      } catch (ArtifactNotFoundException e) {
        throw new IOException(String.format("Artifact %s is not found", artifactId), e);
      }
    }

    HttpRequest.Builder requestBuilder =
      remoteClientInternal.requestBuilder(
        HttpMethod.GET, String.format("namespaces/%s/artifacts/%s/versions/%s/location",
                                      namespace, artifactInfo.getName(), artifactInfo.getVersion()));

    HttpResponse httpResponse = remoteClientInternal.execute(requestBuilder.build());

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException("Could not get artifact detail, endpoint not found");
    }
    if (httpResponse.getResponseCode() != 200) {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }

    String path = httpResponse.getResponseBodyAsString();
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    if (!location.exists()) {
      throw new IOException(String.format("Artifact location does not exist %s for artifact %s version %s",
                                          path, artifactInfo.getName(), artifactInfo.getVersion()));
    }
    return location;
  }
}
