/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandler;
import io.cdap.cdap.gateway.handlers.ArtifactHttpHandlerInternal;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;


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
  private static final Predicate<Throwable> RETRYABLE_PREDICATE =
      throwable -> (throwable instanceof RetryableException);
  private static final int RETRY_BASE_DELAY_MILLIS = 100;
  private static final int RETRY_MAX_DELAY_MILLIS = 5000;
  private static final int RETRY_TIMEOUT_SECS = 300;

  private final RemoteClient remoteClient;
  private final LocationFactory locationFactory;

  @Inject
  public RemoteArtifactRepositoryReader(LocationFactory locationFactory,
      RemoteClientFactory remoteClientFactory) {
    this(locationFactory,
        remoteClientFactory.createRemoteClient(Constants.Service.APP_FABRIC_HTTP,
            new DefaultHttpRequestConfig(false),
            Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  public RemoteArtifactRepositoryReader(LocationFactory locationFactory,
      RemoteClient remoteClient) {
    this.remoteClient = remoteClient;
    this.locationFactory = locationFactory;
  }

  /**
   * Fetches {@link ArtifactDetail} from {@link AppLifecycleHttpHandler}
   * <p>
   * Note that {@link Location} in {@link ArtifactDescriptor} doesn't get transported over, we need
   * to instantiate it based on the location URI in the received {@link ArtifactDetail} to construct
   * a complete {@link ArtifactDetail}.
   */
  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    HttpResponse httpResponse;
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s",
        artifactId.getNamespace().getId(),
        artifactId.getName(),
        artifactId.getVersion());
    HttpRequest.Builder requestBuilder = getRemoteClient().requestBuilder(HttpMethod.GET, url);
    httpResponse = execute(requestBuilder.build());
    ArtifactDetail detail = GSON.fromJson(httpResponse.getResponseBodyAsString(),
        ARTIFACT_DETAIL_TYPE);

    return new ArtifactDetail(new ArtifactDescriptor(detail.getDescriptor().getNamespace(),
        detail.getDescriptor().getArtifactId(),
        getArtifactLocation(detail.getDescriptor())),
        detail.getMeta());
  }

  /**
   * Returns an input stream for reading the artifact bytes. If no such artifact exists, or an error
   * occurs during reading, an exception is thrown.
   *
   * @param artifactId the id of the artifact to get
   * @return an InputStream for the artifact bytes
   * @throws IOException if there as an exception reading from the store.
   * @throws NotFoundException if the given artifact does not exist
   */
  @Override
  public InputStream newInputStream(Id.Artifact artifactId) throws IOException, NotFoundException {
    String namespaceId = artifactId.getNamespace().getId();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespaceId)) {
      namespaceId = NamespaceId.DEFAULT.getNamespace();
      scope = ArtifactScope.SYSTEM;
    }
    String url = String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
        namespaceId,
        artifactId.getName(),
        artifactId.getVersion(),
        scope);
    HttpURLConnection urlConn = getRemoteClient().openConnection(HttpMethod.GET, url);
    throwIfError(artifactId, urlConn);

    // Use FilterInputStream and override close to ensure the connection is closed once the input stream is closed
    return new FilterInputStream(urlConn.getInputStream()) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          urlConn.disconnect();
        }
      }
    };
  }

  @Override
  public List<ArtifactDetail> getArtifactDetails(ArtifactRange range, int limit,
      ArtifactSortOrder order)
      throws Exception {
    String url = String.format(
        "namespaces/%s/artifacts/%s/versions?lower=%s&upper=%s&limit=%d&order=%s",
        range.getNamespace(),
        range.getName(),
        range.getLower().toString(),
        range.getUpper().toString(),
        limit,
        order.name());
    HttpRequest.Builder requestBuilder = getRemoteClient().requestBuilder(HttpMethod.GET, url);
    HttpResponse httpResponse = execute(requestBuilder.build());
    List<ArtifactDetail> details = GSON.fromJson(httpResponse.getResponseBodyAsString(),
        ARTIFACT_DETAIL_LIST_TYPE);
    List<ArtifactDetail> detailList = new ArrayList<>();
    for (ArtifactDetail detail : details) {
      detailList.add(
          new ArtifactDetail(new ArtifactDescriptor(detail.getDescriptor().getNamespace(),
              detail.getDescriptor().getArtifactId(),
              getArtifactLocation(detail.getDescriptor())),
              detail.getMeta()));
    }
    return detailList;
  }

  /**
   * Allow subclasses to modify artifact locations (e.g. {@link RemoteArtifactRepositoryReaderWithLocalization}
   * to download and cache artifact locally, subsequently return a local location.
   */
  protected Location getArtifactLocation(ArtifactDescriptor descriptor)
      throws IOException, ArtifactNotFoundException {
    return locationFactory.create(descriptor.getLocationURI());
  }

  private HttpResponse execute(HttpRequest request)
      throws IOException, NotFoundException, UnauthorizedException {

    RetryStrategy baseRetryStrategy = RetryStrategies.exponentialDelay(
        RETRY_BASE_DELAY_MILLIS, RETRY_MAX_DELAY_MILLIS, TimeUnit.MILLISECONDS);
    HttpResponse httpResponse =
        Retries.callWithRetries(() -> getRemoteClient().execute(request),
        RetryStrategies.timeLimit(RETRY_TIMEOUT_SECS, TimeUnit.SECONDS, baseRetryStrategy),
        RETRYABLE_PREDICATE);
    if (httpResponse.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(httpResponse.getResponseBodyAsString());
    }
    if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(httpResponse.getResponseBodyAsString());
    }
    return httpResponse;
  }

  /**
   * Validates the response from the given {@link HttpURLConnection} to be 200, or throws exception
   * if it is not 200.
   */
  private void throwIfError(Id.Artifact artifactId,
      HttpURLConnection urlConn) throws IOException, NotFoundException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }
    try (InputStream errorStream = urlConn.getErrorStream()) {
      String errorMsg = "unknown error";
      if (errorStream != null) {
        errorMsg = new String(ByteStreams.toByteArray(errorStream), StandardCharsets.UTF_8);
      }
      switch (responseCode) {
        case HttpURLConnection.HTTP_UNAVAILABLE:
          throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP, errorMsg);
        case HttpURLConnection.HTTP_NOT_FOUND:
          throw new NotFoundException(artifactId);
      }

      throw new IOException(
          String.format(
              "Failed to fetch artifact %s version %s from %s. Response code: %d. Error: %s",
              artifactId.getName(), artifactId.getVersion(), urlConn.getURL(), responseCode,
              errorMsg));
    }
  }

  @VisibleForTesting
  public RemoteClient getRemoteClient() {
    return remoteClient;
  }
}
