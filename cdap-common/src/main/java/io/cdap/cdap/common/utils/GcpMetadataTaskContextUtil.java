/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.ArtifactLocalizer;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for io.cdap.cdap.proto.security.GcpMetadataTaskContext.
 */
public final class GcpMetadataTaskContextUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GcpMetadataTaskContextUtil.class);
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(
      BasicThrowable.class, new BasicThrowableCodec()).create();

  private GcpMetadataTaskContextUtil() {
  }

  /**
   * Returns the base HTTP endpoint of the co-located ArtifactLocalizer sidecar, or {@code null}
   * when {@code artifact.localizer.bind.port} is not configured ({@code <= 0}).
   */
  @Nullable
  public static String getSidecarServiceEndpoint(CConfiguration cConf) {
    int port = cConf.getInt(ArtifactLocalizer.PORT);
    if (port <= 0) {
      return null;
    }
    return String.format("http://%s:%d", InetAddress.getLoopbackAddress().getHostName(), port);
  }

  private static boolean isNamespacedServiceAccountsEnabled(FeatureFlagsProvider featureFlags) {
    return Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlags)
        || Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.isEnabled(featureFlags);
  }

  /**
   * Sets the GcpMetadataTaskContext in the sidecar metadata server if NAMESPACED_SERVICE_ACCOUNTS
   * or NAMESPACED_SERVICE_ACCOUNTS_STRICT is enabled and returns the one-time unlock token required
   * to clear the context.
   */
  @Nullable
  public static String setGcpMetadataTaskContext(NamespaceId namespaceId, CConfiguration cConf)
      throws IOException {
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    String sidecarEndpoint = getSidecarServiceEndpoint(cConf);
    if (!isNamespacedServiceAccountsEnabled(featureFlagsProvider)
        || sidecarEndpoint == null) {
      return null;
    }
    GcpMetadataTaskContext gcpMetadataTaskContext = new GcpMetadataTaskContext(
        namespaceId.getNamespace(),
        SecurityRequestContext.getUserId(), SecurityRequestContext.getUserIp(),
        SecurityRequestContext.getUserCredential());
    String setContextEndpoint = String.format("%s/set-context", sidecarEndpoint);
    HttpRequest httpRequest =
        HttpRequest.put(new URL(setContextEndpoint))
            .withBody(GSON.toJson(gcpMetadataTaskContext))
            .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .build();
    HttpResponse tokenResponse = HttpRequests.execute(httpRequest);
    LOG.debug("Set namespace '{}' response: {}", namespaceId.getNamespace(),
        tokenResponse.getResponseCode());
    if (tokenResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format(
          "Failed to set GCP metadata task context for namespace '%s' (HTTP %d): %s",
          namespaceId.getNamespace(), tokenResponse.getResponseCode(),
          tokenResponse.getResponseBodyAsString()));
    }
    return tokenResponse.getHeaders().entries().stream()
        .filter(e -> Constants.Security.Headers.METADATA_UNLOCK_TOKEN.equalsIgnoreCase(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  /**
   * Clears the GcpMetadataTaskContext in the sidecar metadata server if NAMESPACED_SERVICE_ACCOUNTS
   * or NAMESPACED_SERVICE_ACCOUNTS_STRICT is enabled.
   */
  public static void clearGcpMetadataTaskContext(CConfiguration cConf,
      @Nullable String unlockToken) throws IOException {
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    String sidecarEndpoint = getSidecarServiceEndpoint(cConf);
    if (!isNamespacedServiceAccountsEnabled(featureFlagsProvider)
        || sidecarEndpoint == null) {
      return;
    }
    String clearContextEndpoint = String.format("%s/clear-context", sidecarEndpoint);
    HttpRequest.Builder requestBuilder = HttpRequest.delete(new URL(clearContextEndpoint));
    if (unlockToken != null) {
      requestBuilder.addHeader(Constants.Security.Headers.METADATA_UNLOCK_TOKEN, unlockToken);
    }
    HttpResponse tokenResponse = HttpRequests.execute(requestBuilder.build());
    LOG.debug("Clear context response: {}", tokenResponse.getResponseCode());
    if (tokenResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException(String.format(
          "Failed to clear GCP metadata task context (HTTP %d): %s",
          tokenResponse.getResponseCode(), tokenResponse.getResponseBodyAsString()));
    }
  }
}
