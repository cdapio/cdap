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

package io.cdap.cdap.security.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticates master and system-worker extensions against {@code /v3Internal/system/p4sa/token}
 * using a constant-time HMAC header ({@link #HEADER_NAME}) derived from the local
 * {@code tokenManagerKey} file.
 *
 * <p>Because external GCP client libraries (such as {@code ComputeEngineCredentials}) only call a
 * metadata-style {@code token.endpoint} URL without custom authentication headers,
 * {@link #augmentEndpointProperties(CConfiguration, Map)} exposes a cached loopback metadata bridge
 * ({@code http://127.0.0.1:<port>/token}) inside the local JVM that attaches {@link #HEADER_NAME}
 * when forwarding token requests to {@code AppFabric}. No secret ever enters any URL, query string,
 * or {@link CConfiguration}.
 */
public final class P4saTokenEndpointAuth {

  public static final String HMAC_PURPOSE = "cdap-system-p4sa-token-v1";
  public static final String HEADER_NAME = "X-CDAP-System-P4SA-Token";

  private static final String HMAC_ALGORITHM = "HmacSHA256";
  private static final String TOKEN_ENDPOINT_KEY = "token.endpoint";
  private static final String TOKEN_ENDPOINT_SUFFIX = ".token.endpoint";
  private static final String METADATA_FLAVOR_HEADER = "Metadata-Flavor";
  private static final String METADATA_FLAVOR_GOOGLE = "Google";
  private static final int CONNECT_TIMEOUT_MS = 15000;
  private static final int READ_TIMEOUT_MS = 15000;

  private static final Logger LOG = LoggerFactory.getLogger(P4saTokenEndpointAuth.class);
  private static final KeyIdentifierCodec KEY_IDENTIFIER_CODEC = new KeyIdentifierCodec();
  private static final Object BRIDGE_LOCK = new Object();
  private static final Map<BridgeKey, LocalBridge> BRIDGES = new ConcurrentHashMap<>();
  private static final Set<String> ACTIVE_BRIDGE_URLS = ConcurrentHashMap.newKeySet();
  private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean(false);

  private P4saTokenEndpointAuth() {
  }

  /**
   * Computes the URL-safe Base64 HMAC token when the configured keyfile exists on disk, or returns
   * {@code null} when the keyfile is absent or unreadable.
   */
  @Nullable
  public static String computeToken(@Nullable CConfiguration cConf) {
    java.nio.file.Path keyFilePath = resolveKeyFilePath(cConf);
    return keyFilePath == null ? null : computeToken(keyFilePath);
  }

  /**
   * Verifies {@code providedToken} in constant time against the keyfile configured in
   * {@code cConf}.
   */
  public static boolean verifyToken(@Nullable CConfiguration cConf,
      @Nullable String providedToken) {
    if (Strings.isNullOrEmpty(providedToken)) {
      return false;
    }
    String expectedToken = computeToken(cConf);
    if (Strings.isNullOrEmpty(expectedToken)) {
      return false;
    }
    return MessageDigest.isEqual(
        expectedToken.getBytes(StandardCharsets.UTF_8),
        providedToken.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns a new map copying {@code props} where {@code "token.endpoint"} and any key ending in
   * {@code ".token.endpoint"} points to a local loopback metadata bridge
   * ({@code http://127.0.0.1:<port>/token}) when the configured keyfile exists on disk. Never
   * mutates {@code cConf} or {@code props}.
   */
  public static Map<String, String> augmentEndpointProperties(@Nullable CConfiguration cConf,
      @Nullable Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, String> augmented = new HashMap<>(props);
    java.nio.file.Path keyFilePath = resolveKeyFilePath(cConf);
    if (keyFilePath == null || Strings.isNullOrEmpty(computeToken(keyFilePath))) {
      return augmented;
    }
    for (Map.Entry<String, String> entry : augmented.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (isTokenEndpointKey(key) && !Strings.isNullOrEmpty(value)
          && !isLocalBridgeEndpoint(value)) {
        entry.setValue(getOrStartLocalBridgeUrl(keyFilePath, value));
      }
    }
    return augmented;
  }

  @VisibleForTesting
  static void closeAllBridgesForTesting() {
    synchronized (BRIDGE_LOCK) {
      ACTIVE_BRIDGE_URLS.clear();
      for (LocalBridge bridge : BRIDGES.values()) {
        bridge.stopQuietly();
      }
      BRIDGES.clear();
    }
  }

  @Nullable
  private static java.nio.file.Path resolveKeyFilePath(@Nullable CConfiguration cConf) {
    if (cConf == null) {
      return null;
    }
    String keyFilePathStr = cConf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH);
    if (Strings.isNullOrEmpty(keyFilePathStr)) {
      return null;
    }
    java.nio.file.Path keyFilePath = Paths.get(keyFilePathStr).toAbsolutePath().normalize();
    return Files.isRegularFile(keyFilePath) ? keyFilePath : null;
  }

  @Nullable
  private static String computeToken(java.nio.file.Path keyFilePath) {
    if (!Files.isRegularFile(keyFilePath)) {
      return null;
    }
    try {
      KeyIdentifier keyIdentifier = KEY_IDENTIFIER_CODEC.decode(Files.readAllBytes(keyFilePath));
      if (keyIdentifier == null) {
        return null;
      }
      SecretKey secretKey = keyIdentifier.getKey();
      byte[] keyBytes = secretKey == null ? null : secretKey.getEncoded();
      if (keyBytes == null || keyBytes.length == 0) {
        return null;
      }
      Mac mac = Mac.getInstance(HMAC_ALGORITHM);
      mac.init(new SecretKeySpec(keyBytes, HMAC_ALGORITHM));
      byte[] digest = mac.doFinal(HMAC_PURPOSE.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (IOException | GeneralSecurityException | RuntimeException e) {
      LOG.warn("Failed to derive P4SA endpoint token from keyfile {}", keyFilePath, e);
      return null;
    }
  }

  private static boolean isTokenEndpointKey(@Nullable String key) {
    return key != null
        && (TOKEN_ENDPOINT_KEY.equals(key) || key.endsWith(TOKEN_ENDPOINT_SUFFIX));
  }

  private static boolean isLocalBridgeEndpoint(String endpointUrl) {
    return ACTIVE_BRIDGE_URLS.contains(stripQueryAndFragment(endpointUrl));
  }

  private static String stripQueryAndFragment(String url) {
    int endIdx = url.length();
    int queryIdx = url.indexOf('?');
    if (queryIdx >= 0 && queryIdx < endIdx) {
      endIdx = queryIdx;
    }
    int hashIdx = url.indexOf('#');
    if (hashIdx >= 0 && hashIdx < endIdx) {
      endIdx = hashIdx;
    }
    return url.substring(0, endIdx);
  }

  private static String getOrStartLocalBridgeUrl(java.nio.file.Path keyFilePath,
      String upstreamEndpointUrl) {
    BridgeKey bridgeKey = new BridgeKey(keyFilePath.toString(), upstreamEndpointUrl);
    LocalBridge existing = BRIDGES.get(bridgeKey);
    if (existing != null) {
      return existing.localEndpointUrl;
    }
    synchronized (BRIDGE_LOCK) {
      existing = BRIDGES.get(bridgeKey);
      if (existing != null) {
        return existing.localEndpointUrl;
      }
      registerShutdownHookIfNeeded();
      NettyHttpService server = NettyHttpService.builder("p4sa-token-bridge")
          .setHost(InetAddress.getLoopbackAddress().getHostAddress())
          .setPort(0)
          .setBossThreadPoolSize(1)
          .setWorkerThreadPoolSize(1)
          .setExecThreadPoolSize(2)
          .setHttpHandlers(new LocalP4saMetadataHandler(keyFilePath, upstreamEndpointUrl))
          .build();
      try {
        server.start();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to start local P4SA metadata bridge", e);
      }
      String localUrl = String.format("http://127.0.0.1:%d/token",
          server.getBindAddress().getPort());
      LocalBridge created = new LocalBridge(server, localUrl);
      BRIDGES.put(bridgeKey, created);
      ACTIVE_BRIDGE_URLS.add(localUrl);
      return localUrl;
    }
  }

  private static void registerShutdownHookIfNeeded() {
    if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(new Thread(
          P4saTokenEndpointAuth::closeAllBridgesForTesting, "p4sa-token-bridge-shutdown"));
    }
  }

  @Nullable
  private static String extractRawQuery(String uri) {
    int queryIdx = uri.indexOf('?');
    if (queryIdx < 0 || queryIdx + 1 >= uri.length()) {
      return null;
    }
    String rawQuery = uri.substring(queryIdx + 1);
    int hashIdx = rawQuery.indexOf('#');
    return hashIdx >= 0 ? rawQuery.substring(0, hashIdx) : rawQuery;
  }

  private static String appendRawQuery(String upstreamUrl, @Nullable String rawQuery) {
    if (Strings.isNullOrEmpty(rawQuery)) {
      return upstreamUrl;
    }
    String fragment = "";
    String withoutFragment = upstreamUrl;
    int hashIdx = upstreamUrl.indexOf('#');
    if (hashIdx >= 0) {
      fragment = upstreamUrl.substring(hashIdx);
      withoutFragment = upstreamUrl.substring(0, hashIdx);
    }
    char separator = withoutFragment.indexOf('?') >= 0 ? '&' : '?';
    return withoutFragment + separator + rawQuery + fragment;
  }

  private static byte[] readResponseBody(HttpURLConnection conn, int statusCode)
      throws IOException {
    InputStream stream = statusCode >= 400 ? conn.getErrorStream() : conn.getInputStream();
    if (stream == null) {
      return new byte[0];
    }
    try (InputStream in = stream) {
      return ByteStreams.toByteArray(in);
    }
  }

  /**
   * Loopback-only metadata handler forwarding local extension token requests to {@code AppFabric}
   * with {@link #HEADER_NAME}.
   */
  public static final class LocalP4saMetadataHandler extends AbstractHttpHandler {

    private final java.nio.file.Path keyFilePath;
    private final String upstreamEndpointUrl;

    LocalP4saMetadataHandler(java.nio.file.Path keyFilePath, String upstreamEndpointUrl) {
      this.keyFilePath = keyFilePath;
      this.upstreamEndpointUrl = upstreamEndpointUrl;
    }

    /**
     * Relays {@code GET /token} to the configured upstream {@code AppFabric} P4SA token endpoint.
     */
    @GET
    @Path("/token")
    public void getToken(HttpRequest request, HttpResponder responder) {
      String hmacToken = computeToken(keyFilePath);
      if (Strings.isNullOrEmpty(hmacToken)) {
        responder.sendString(HttpResponseStatus.FORBIDDEN,
            "Forbidden: system P4SA keyfile is unavailable.");
        return;
      }
      String rawQuery = extractRawQuery(request.uri());
      HttpURLConnection conn = null;
      try {
        URL targetUrl = new URL(appendRawQuery(upstreamEndpointUrl, rawQuery));
        conn = (HttpURLConnection) targetUrl.openConnection();
        if (conn instanceof HttpsURLConnection) {
          new HttpsEnabler().setTrustAll(true).enable((HttpsURLConnection) conn);
        }
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setDoInput(true);
        conn.setRequestProperty(METADATA_FLAVOR_HEADER, METADATA_FLAVOR_GOOGLE);
        conn.setRequestProperty(HEADER_NAME, hmacToken);

        int statusCode = conn.getResponseCode();
        byte[] body = readResponseBody(conn, statusCode);
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        String contentType = conn.getContentType();
        if (!Strings.isNullOrEmpty(contentType)) {
          headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
        responder.sendByteArray(HttpResponseStatus.valueOf(statusCode), body, headers);
      } catch (IOException e) {
        LOG.warn("Failed to relay system P4SA token request to {}",
            stripQueryAndFragment(upstreamEndpointUrl), e);
        responder.sendString(HttpResponseStatus.BAD_GATEWAY,
            "Failed to relay system P4SA token request.");
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }
  }

  private static final class BridgeKey {
    private final String keyFilePath;
    private final String upstreamEndpointUrl;

    BridgeKey(String keyFilePath, String upstreamEndpointUrl) {
      this.keyFilePath = keyFilePath;
      this.upstreamEndpointUrl = upstreamEndpointUrl;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BridgeKey)) {
        return false;
      }
      BridgeKey that = (BridgeKey) o;
      return Objects.equals(keyFilePath, that.keyFilePath)
          && Objects.equals(upstreamEndpointUrl, that.upstreamEndpointUrl);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyFilePath, upstreamEndpointUrl);
    }
  }

  private static final class LocalBridge {
    private final NettyHttpService server;
    private final String localEndpointUrl;

    LocalBridge(NettyHttpService server, String localEndpointUrl) {
      this.server = server;
      this.localEndpointUrl = localEndpointUrl;
    }

    void stopQuietly() {
      try {
        server.stop();
      } catch (Exception e) {
        LOG.debug("Failed to stop local P4SA metadata bridge {}", localEndpointUrl, e);
      }
    }
  }
}
