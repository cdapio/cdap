/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.Idempotency;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;

/**
 * Fetches connection plugin properties from the pipeline studio service at program launch time
 * using the system identity and a shared per-launch deadline across all reads.
 */
public class ConnectionPropertiesFetcher {

  // Keep in sync with io.cdap.cdap.etl.common.Constants.PIPELINEID and STUDIO_SERVICE_NAME,
  // and io.cdap.cdap.etl.common.ConnectionMacroEvaluator.
  private static final String PIPELINE_APP_NAME = "pipeline";
  private static final String STUDIO_SERVICE_NAME = "studio";

  private static final String STUDIO_DISCOVERY_NAME = ServiceDiscoverable.getName(
      NamespaceId.SYSTEM.getNamespace(), PIPELINE_APP_NAME, ProgramType.SERVICE,
      STUDIO_SERVICE_NAME);
  private static final String STUDIO_BASE_PATH = String.format(
      "%s/namespaces/%s/apps/%s/services/%s/methods/",
      Constants.Gateway.API_VERSION_3_TOKEN, NamespaceId.SYSTEM.getNamespace(),
      PIPELINE_APP_NAME, STUDIO_SERVICE_NAME);

  private static final long DEFAULT_LAUNCH_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(15);
  private static final Gson GSON = new Gson();

  private final RemoteClientFactory remoteClientFactory;
  private final long launchTimeoutMillis;
  private final LongSupplier nanoTimeSupplier;

  @Inject
  public ConnectionPropertiesFetcher(RemoteClientFactory remoteClientFactory) {
    this(remoteClientFactory, DEFAULT_LAUNCH_TIMEOUT_MILLIS, System::nanoTime);
  }

  @VisibleForTesting
  ConnectionPropertiesFetcher(RemoteClientFactory remoteClientFactory,
      long launchTimeoutMillis, LongSupplier nanoTimeSupplier) {
    this.remoteClientFactory = Objects.requireNonNull(remoteClientFactory, "remoteClientFactory");
    this.launchTimeoutMillis = launchTimeoutMillis;
    this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier, "nanoTimeSupplier");
  }

  /**
   * Creates a {@link RuntimeScopesExtractor.ConnectionReader} scoped to {@code namespace} with a
   * single deadline shared across all connection reads for one launch.
   */
  public RuntimeScopesExtractor.ConnectionReader forNamespace(String namespace) {
    Objects.requireNonNull(namespace, "namespace");
    long deadlineNanos = nanoTimeSupplier.getAsLong()
        + TimeUnit.MILLISECONDS.toNanos(launchTimeoutMillis);
    return connectionName -> readConnection(namespace, connectionName, deadlineNanos);
  }

  private Optional<Map<String, String>> readConnection(String namespace, String connectionName,
      long deadlineNanos) throws IOException {
    long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
        deadlineNanos - nanoTimeSupplier.getAsLong());
    if (remainingMillis <= 0) {
      throw new IOException(String.format(
          "Launch connection-read deadline exceeded before reading connection '%s' in namespace '%s'",
          connectionName, namespace));
    }
    int timeoutMs = (int) Math.min(Integer.MAX_VALUE, remainingMillis);
    HttpRequestConfig requestConfig = new HttpRequestConfig(timeoutMs, timeoutMs, false);

    // Match ConnectionMacroEvaluator URL-encoding (spaces become '+').
    String encodedName = URLEncoder.encode(connectionName, StandardCharsets.UTF_8.name());
    String resource = String.format("v1/contexts/%s/connections/%s", namespace, encodedName);

    HttpResponse response;
    try {
      response = AuthorizationUtil.callAsSystem(() -> {
        RemoteClient remoteClient = remoteClientFactory.createRemoteClient(
            STUDIO_DISCOVERY_NAME, requestConfig, STUDIO_BASE_PATH);
        HttpRequest request = remoteClient.requestBuilder(HttpMethod.GET, resource).build();
        return remoteClient.execute(request, Idempotency.NONE);
      });
    } catch (ServiceUnavailableException | ServiceException | UnauthorizedException e) {
      throw new IOException("Failed to fetch connection '" + connectionName
          + "' in namespace '" + namespace + "'", e);
    }

    int statusCode = response.getResponseCode();
    if (statusCode == HttpURLConnection.HTTP_NOT_FOUND
        || statusCode == HttpURLConnection.HTTP_BAD_REQUEST) {
      return Optional.empty();
    }
    if (statusCode != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format(
          "Failed to fetch connection '%s' in namespace '%s': HTTP %d",
          connectionName, namespace, statusCode));
    }

    ConnectionResponse parsed;
    try {
      parsed = GSON.fromJson(response.getResponseBodyAsString(StandardCharsets.UTF_8),
          ConnectionResponse.class);
    } catch (JsonParseException | StackOverflowError e) {
      throw new IOException("Failed to parse connection '" + connectionName
          + "' in namespace '" + namespace + "'", e);
    }
    if (parsed == null) {
      throw new IOException("Empty response for connection '" + connectionName
          + "' in namespace '" + namespace + "'");
    }
    if (parsed.plugin == null || parsed.plugin.properties == null) {
      return Optional.of(Collections.emptyMap());
    }
    return Optional.of(parsed.plugin.properties);
  }

  // Keep field names in sync with io.cdap.cdap.etl.proto.connection.Connection and PluginMeta.
  private static final class ConnectionResponse {
    @Nullable
    private PluginResponse plugin;
  }

  private static final class PluginResponse {
    @Nullable
    private Map<String, String> properties;
  }
}
