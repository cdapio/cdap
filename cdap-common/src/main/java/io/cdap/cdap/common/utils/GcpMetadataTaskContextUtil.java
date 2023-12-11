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
import java.net.InetAddress;
import java.net.URL;
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

  private static String getSidecarMetadataServiceEndpoint(CConfiguration cConf) {
    if (cConf.getInt(ArtifactLocalizer.PORT) < 0) {
      return null;
    }
    return String.format("http://%s:%s", InetAddress.getLoopbackAddress().getHostName(),
        cConf.get(ArtifactLocalizer.PORT));
  }

  /**
   * Sets the GcpMetadataTaskContext in the sidecar metadata server if NAMESPACED_SERVICE_ACCOUNTS
   * feature is enabled.
   */
  public static void setGcpMetadataTaskContext(NamespaceId namespaceId, CConfiguration cConf)
      throws IOException {
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    if (!Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)
        || getSidecarMetadataServiceEndpoint(cConf) == null) {
      return;
    }
    GcpMetadataTaskContext gcpMetadataTaskContext = new GcpMetadataTaskContext(
        namespaceId.getNamespace(),
        SecurityRequestContext.getUserId(), SecurityRequestContext.getUserIp(),
        SecurityRequestContext.getUserCredential());
    String setContextEndpoint = String.format("%s/set-context",
        getSidecarMetadataServiceEndpoint(cConf));
    HttpRequest httpRequest =
        HttpRequest.put(new URL(setContextEndpoint))
            .withBody(GSON.toJson(gcpMetadataTaskContext))
            .addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .build();
    HttpResponse tokenResponse = HttpRequests.execute(httpRequest);
    LOG.debug("Set namespace '{}' response: {}", namespaceId.getNamespace(),
        tokenResponse.getResponseCode());
  }

  /**
   * Clears the GcpMetadataTaskContext in the sidecar metadata server if NAMESPACED_SERVICE_ACCOUNTS
   * feature is enabled.
   */
  public static void clearGcpMetadataTaskContext(CConfiguration cConf) throws IOException {
    FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    if (!Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)
        || getSidecarMetadataServiceEndpoint(cConf) == null) {
      return;
    }
    String clearContextEndpoint = String.format("%s/clear-context",
        getSidecarMetadataServiceEndpoint(cConf));
    HttpRequest httpRequest = HttpRequest.delete(new URL(clearContextEndpoint)).build();
    HttpResponse tokenResponse = HttpRequests.execute(httpRequest);
    LOG.debug("Clear context response: {}", tokenResponse.getResponseCode());
  }
}
