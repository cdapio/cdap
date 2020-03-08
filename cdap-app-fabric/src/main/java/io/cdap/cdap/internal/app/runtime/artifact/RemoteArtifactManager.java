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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.logging.LocationAwareWrapperLogger;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Implementation for {@link io.cdap.cdap.api.artifact.ArtifactManager}
 * communicating with {@link io.cdap.cdap.gateway.handlers.ArtifactHttpHandler} and returning artifact info.
 */
public final class RemoteArtifactManager extends AbstractArtifactManager {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteArtifactManager.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final Type ARTIFACT_INFO_LIST_TYPE = new TypeToken<List<ArtifactInfo>>() { }.getType();
  private final LocationFactory locationFactory;
  private final AuthenticationContext authenticationContext;
  private final NamespaceId namespaceId;
  private final RetryStrategy retryStrategy;
  private final boolean authorizationEnabled;
  private final RemoteClient remoteClient;

  @Inject
  RemoteArtifactManager(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
                        LocationFactory locationFactory, AuthenticationContext authenticationContext,
                        @Assisted NamespaceId namespaceId, @Assisted RetryStrategy retryStrategy) {
    super(cConf);
    this.locationFactory = locationFactory;
    this.authenticationContext = authenticationContext;
    this.namespaceId = namespaceId;
    this.retryStrategy = retryStrategy;
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         new DefaultHttpRequestConfig(false),
                                         String.format("%s", Constants.Gateway.API_VERSION_3));
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace and also the artifacts in system
   * namespace.
   *
   * If the app-fabric service is unavailable, it will be retried based on the passed in retry strategy.
   *
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  @Override
  public List<ArtifactInfo> listArtifacts() throws IOException {
    return listArtifacts(namespaceId.getNamespace());
  }

  /**
   * For the specified namespace, return the available artifacts in the namespace and also the artifacts in system
   * namespace.
   *
   * If the app-fabric service is unavailable, it will be retried based on the passed in retry strategy.
   *
   * @return {@link List<ArtifactInfo>}
   * @throws IOException If there are any exception while retrieving artifacts
   */
  @Override
  public List<ArtifactInfo> listArtifacts(String namespace) throws IOException {
    return Retries.callWithRetries(() -> {
      HttpRequest.Builder requestBuilder =
        remoteClient.requestBuilder(HttpMethod.GET,
                                    String.format("namespaces/%s/artifact-internals/artifacts", namespace));
      // add header if auth is enabled
      if (authorizationEnabled) {
        requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
      }
      return getArtifactsList(requestBuilder.build());
    }, retryStrategy);
  }

  private List<ArtifactInfo> getArtifactsList(HttpRequest httpRequest) throws IOException {
    HttpResponse httpResponse = remoteClient.execute(httpRequest);

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException("Could not list artifacts, endpoint not found");
    }

    if (httpResponse.getResponseCode() == 200) {
      return GSON.fromJson(httpResponse.getResponseBodyAsString(), ARTIFACT_INFO_LIST_TYPE);
    }

    throw new IOException("Exception while getting artifacts list " + httpResponse.getResponseBodyAsString());
  }

  @Override
  public Location getArtifactLocation(ArtifactSummary artifactSummary,
                                      @Nullable String artifactNamespace) throws IOException {
    String namespace;
    if (ArtifactScope.SYSTEM.equals(artifactSummary.getScope())) {
      namespace = NamespaceId.SYSTEM.getNamespace();
    } else if (artifactNamespace != null) {
      namespace = artifactNamespace;
    } else {
      namespace = namespaceId.getNamespace();
    }

    HttpRequest.Builder requestBuilder =
      remoteClient.requestBuilder(
        HttpMethod.GET, String.format("namespaces/%s/artifact-internals/artifacts/%s/versions/%s/location",
                                      namespace, artifactSummary.getName(), artifactSummary.getVersion()));
    // add header if auth is enabled
    if (authorizationEnabled) {
      requestBuilder.addHeader(Constants.Security.Headers.USER_ID, authenticationContext.getPrincipal().getName());
    }

    HttpResponse httpResponse = remoteClient.execute(requestBuilder.build());

    if (httpResponse.getResponseCode() == HttpResponseStatus.NOT_FOUND.code()) {
      throw new IOException("Could not get artifact detail, endpoint not found");
    }
    if (httpResponse.getResponseCode() != 200) {
      throw new IOException(String.format("Exception while getting artifacts list %s",
                                          httpResponse.getResponseBodyAsString()));
    }

    String path = httpResponse.getResponseBodyAsString();
    Location location = Locations.getLocationFromAbsolutePath(locationFactory, path);
    LOG.debug("wyzhang: RemoteArtifactManager location got = " + location.toString());
    LOG.debug("wyzhang: RemoteArtifactManager locationFactory = " + locationFactory.getClass().getName());
    if (!location.exists()) {
      throw new IOException(String.format("Artifact Location does not exist %s for artifact %s version %s",
                                          path, artifactSummary.getName(), artifactSummary.getVersion()));
    }
    return location;
  }
}
