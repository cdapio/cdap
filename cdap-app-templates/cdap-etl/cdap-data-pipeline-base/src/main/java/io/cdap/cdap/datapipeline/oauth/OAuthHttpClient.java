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

package io.cdap.cdap.datapipeline.oauth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

/**
 * Executes outbound OAuth HTTP requests with SSRF validation, disabled redirects, DNS-rebinding
 * protection via IP-pinned sockets, and bounded response reading.
 */
public final class OAuthHttpClient {
  public static final int DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024;
  static final int MAX_CONNECT_ATTEMPTS = 4;
  private static final int READ_BUFFER_BYTES = 4096;

  private final OAuthUrlValidator urlValidator;
  private final SSLSocketFactory sslSocketFactory;
  private final int connectTimeoutMs;
  private final int readTimeoutMs;
  private final int maxResponseBytes;

  public OAuthHttpClient(OAuthUrlValidator urlValidator) {
    this(urlValidator, HttpsURLConnection.getDefaultSSLSocketFactory(),
         HttpRequestConfig.DEFAULT.getConnectTimeout(),
         HttpRequestConfig.DEFAULT.getReadTimeout(),
         DEFAULT_MAX_RESPONSE_BYTES);
  }

  @VisibleForTesting
  public OAuthHttpClient(OAuthUrlValidator urlValidator, SSLSocketFactory sslSocketFactory,
                         int connectTimeoutMs, int readTimeoutMs, int maxResponseBytes) {
    this.urlValidator = Objects.requireNonNull(urlValidator, "urlValidator must not be null");
    this.sslSocketFactory = Objects.requireNonNull(sslSocketFactory, "sslSocketFactory must not be null");
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.maxResponseBytes = maxResponseBytes;
  }

  /**
   * Immutable HTTP response returned by {@link OAuthHttpClient}.
   */
  public static final class Response {
    private final int responseCode;
    @Nullable
    private final String responseMessage;
    private final String responseBody;

    private Response(int responseCode, @Nullable String responseMessage, String responseBody) {
      this.responseCode = responseCode;
      this.responseMessage = responseMessage;
      this.responseBody = responseBody;
    }

    public int getResponseCode() {
      return responseCode;
    }

    @Nullable
    public String getResponseMessage() {
      return responseMessage;
    }

    public String getResponseBodyAsString() {
      return responseBody;
    }
  }

  /**
   * Validates the request URL, pins the connection to validated resolved IPs, disables redirects,
   * and executes the request. Retries up to {@link #MAX_CONNECT_ATTEMPTS} addresses (families
   * interleaved) within one shared connect timeout, and never retries once a TCP connection opens.
   */
  public Response execute(HttpRequest request, OAuthUrlValidator.AllowedHosts allowedHosts,
                          boolean readBodyOnSuccess) throws IOException {
    OAuthUrlValidator.ValidatedUrl validatedUrl = urlValidator.validate(
        request.getURL().toString(), OAuthUrlValidator.HostResolution.REQUIRE_RESOLVABLE, allowedHosts);
    byte[] body = readRequestBody(request);
    List<InetAddress> candidates = selectConnectCandidates(validatedUrl.getResolvedAddresses(), MAX_CONNECT_ATTEMPTS);
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(connectTimeoutMs);
    IOException lastConnectFailure = null;
    for (int i = 0; i < candidates.size(); i++) {
      int attemptTimeoutMs = connectTimeoutMs;
      if (connectTimeoutMs > 0) {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        if (remainingMs <= 0) {
          break;
        }
        attemptTimeoutMs = (int) Math.max(1L, remainingMs / (candidates.size() - i));
      }
      AtomicBoolean connected = new AtomicBoolean(false);
      try {
        return executePinned(request, body, validatedUrl, candidates.get(i), attemptTimeoutMs, connected,
                             readBodyOnSuccess);
      } catch (IOException e) {
        if (connected.get()) {
          throw e;
        }
        lastConnectFailure = e;
      }
    }
    throw lastConnectFailure != null ? lastConnectFailure : new IOException("No resolved IP addresses available");
  }

  /**
   * Returns up to {@code maxAttempts} addresses, alternating address families starting with the first
   * record's family so one unreachable family cannot exhaust all attempts.
   */
  @VisibleForTesting
  static List<InetAddress> selectConnectCandidates(List<InetAddress> resolved, int maxAttempts) {
    if (resolved.isEmpty()) {
      return resolved;
    }
    boolean firstIsIpv6 = resolved.get(0) instanceof Inet6Address;
    List<InetAddress> primary = new ArrayList<>();
    List<InetAddress> secondary = new ArrayList<>();
    for (InetAddress address : resolved) {
      ((address instanceof Inet6Address) == firstIsIpv6 ? primary : secondary).add(address);
    }
    List<InetAddress> selected = new ArrayList<>(Math.min(resolved.size(), maxAttempts));
    for (int i = 0; selected.size() < maxAttempts && (i < primary.size() || i < secondary.size()); i++) {
      if (i < primary.size()) {
        selected.add(primary.get(i));
      }
      if (i < secondary.size() && selected.size() < maxAttempts) {
        selected.add(secondary.get(i));
      }
    }
    return selected;
  }

  private Response executePinned(HttpRequest request, @Nullable byte[] body,
                                 OAuthUrlValidator.ValidatedUrl validatedUrl, InetAddress pinnedAddress,
                                 int attemptTimeoutMs, AtomicBoolean connected,
                                 boolean readBodyOnSuccess) throws IOException {
    boolean isHttps = "https".equalsIgnoreCase(validatedUrl.getUrl().getProtocol());
    HttpURLConnection connection = openPinnedConnection(validatedUrl, pinnedAddress, connected);
    try {
      configureConnection(connection, request, body, attemptTimeoutMs);
      if (!isHttps) {
        connection.connect();
        connected.set(true);
      }
      if (body != null) {
        try (OutputStream output = connection.getOutputStream()) {
          output.write(body);
        }
      }
      int responseCode = connection.getResponseCode();
      connected.set(true);
      String responseBody = readResponseBody(connection, responseCode, readBodyOnSuccess);
      return new Response(responseCode, connection.getResponseMessage(), responseBody);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openPinnedConnection(OAuthUrlValidator.ValidatedUrl validatedUrl,
                                                 InetAddress pinnedAddress,
                                                 AtomicBoolean connected) throws IOException {
    URL targetUrl = validatedUrl.getUrl();
    int effectivePort = validatedUrl.getEffectivePort();
    if ("https".equalsIgnoreCase(targetUrl.getProtocol())) {
      HttpsURLConnection https = (HttpsURLConnection) targetUrl.openConnection(Proxy.NO_PROXY);
      https.setSSLSocketFactory(new PinnedSSLSocketFactory(
          sslSocketFactory, validatedUrl.getNormalizedHost(), pinnedAddress, effectivePort, connected));
      return https;
    }
    URL pinnedHttpUrl = new URL("http", InetAddresses.toUriString(pinnedAddress), effectivePort, targetUrl.getFile());
    return (HttpURLConnection) pinnedHttpUrl.openConnection(Proxy.NO_PROXY);
  }

  private void configureConnection(HttpURLConnection connection, HttpRequest request, @Nullable byte[] body,
                                   int attemptTimeoutMs) throws IOException {
    connection.setInstanceFollowRedirects(false);
    connection.setRequestMethod(request.getMethod().name());
    connection.setConnectTimeout(attemptTimeoutMs);
    connection.setReadTimeout(readTimeoutMs);
    connection.setRequestProperty("Connection", "close");
    connection.setRequestProperty("Accept-Encoding", "identity");
    if (request.getHeaders() != null) {
      for (Map.Entry<String, String> header : request.getHeaders().entries()) {
        if (!"Connection".equalsIgnoreCase(header.getKey())) {
          connection.addRequestProperty(header.getKey(), header.getValue());
        }
      }
    }
    if (body != null) {
      connection.setDoOutput(true);
      // Stops HttpURLConnection from silently replaying POSTs (sun.net.http.retryPost) on connection drop.
      connection.setFixedLengthStreamingMode(body.length);
    }
  }

  @Nullable
  private static byte[] readRequestBody(HttpRequest request) throws IOException {
    ContentProvider<? extends InputStream> body = request.getBody();
    if (body == null) {
      return null;
    }
    try (InputStream input = body.getInput()) {
      return ByteStreams.toByteArray(input);
    }
  }

  private String readResponseBody(HttpURLConnection connection, int responseCode,
                                  boolean readBodyOnSuccess) throws IOException {
    if (responseCode < 200 || responseCode >= 300) {
      closeQuietly(connection.getErrorStream());
      return "";
    }
    InputStream stream = connection.getInputStream();
    if (!readBodyOnSuccess || stream == null) {
      closeQuietly(stream);
      return "";
    }
    String encoding = Strings.nullToEmpty(connection.getContentEncoding()).trim().toLowerCase(Locale.ROOT);
    InputStream boundedWire = new BoundedInputStream(stream, maxResponseBytes);
    try (InputStream decoded = ("gzip".equals(encoding) || "x-gzip".equals(encoding))
        ? new GZIPInputStream(boundedWire) : boundedWire) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[READ_BUFFER_BYTES];
      int total = 0;
      int read;
      while ((read = decoded.read(buffer)) != -1) {
        total += read;
        if (total > maxResponseBytes) {
          throw new IOException("OAuth response body exceeds maximum size of " + maxResponseBytes + " bytes");
        }
        out.write(buffer, 0, read);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private static void closeQuietly(@Nullable InputStream stream) {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException ignored) {
        // Best-effort cleanup for discarded response stream
      }
    }
  }

  private static final class BoundedInputStream extends FilterInputStream {
    private final int maxBytes;
    private int bytesRead;

    private BoundedInputStream(InputStream in, int maxBytes) {
      super(in);
      this.maxBytes = maxBytes;
    }

    @Override
    public int read() throws IOException {
      int value = super.read();
      if (value != -1) {
        record(1);
      }
      return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      if (read > 0) {
        record(read);
      }
      return read;
    }

    private void record(int count) throws IOException {
      bytesRead += count;
      if (bytesRead > maxBytes) {
        throw new IOException("OAuth response body exceeds maximum size of " + maxBytes + " bytes");
      }
    }
  }

  /**
   * Plain socket that always connects to the pre-validated IP, ignoring any DNS lookup by HttpURLConnection.
   */
  private static final class PinnedSocket extends Socket {
    private final InetAddress pinnedAddress;
    private final int pinnedPort;
    private final AtomicBoolean connected;

    private PinnedSocket(InetAddress pinnedAddress, int pinnedPort, AtomicBoolean connected) {
      this.pinnedAddress = pinnedAddress;
      this.pinnedPort = pinnedPort;
      this.connected = connected;
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
      connect(endpoint, 0);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
      super.connect(new InetSocketAddress(pinnedAddress, pinnedPort), timeout);
      connected.set(true);
    }
  }

  /**
   * {@link SSLSocketFactory} that pins TCP connections to a validated {@link InetAddress} while
   * preserving TLS SNI and hostname verification for the original hostname.
   */
  private static final class PinnedSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;
    private final String expectedHost;
    private final InetAddress pinnedAddress;
    private final int pinnedPort;
    private final AtomicBoolean connected;

    private PinnedSSLSocketFactory(SSLSocketFactory delegate, String expectedHost,
                                   InetAddress pinnedAddress, int pinnedPort, AtomicBoolean connected) {
      this.delegate = delegate;
      this.expectedHost = expectedHost;
      this.pinnedAddress = pinnedAddress;
      this.pinnedPort = pinnedPort;
      this.connected = connected;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() {
      // Returning a plain Socket causes HttpsClient.afterConnect() to wrap the already-pinned TCP socket
      // via createSocket(Socket, host, port, true), preserving TLS SNI and certificate hostname verification.
      return new PinnedSocket(pinnedAddress, pinnedPort, connected);
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
      return delegate.createSocket(s, expectedHost, pinnedPort, autoClose);
    }

    @Override
    public Socket createSocket(String host, int port) {
      throw unsupported();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
      throw unsupported();
    }

    @Override
    public Socket createSocket(InetAddress host, int port) {
      throw unsupported();
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) {
      throw unsupported();
    }

    private static UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException("Direct socket creation is not supported for pinned OAuth connections");
    }
  }
}
