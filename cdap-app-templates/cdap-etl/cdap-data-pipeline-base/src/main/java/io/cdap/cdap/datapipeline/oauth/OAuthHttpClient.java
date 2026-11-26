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
import com.google.common.io.ByteStreams;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

/**
 * Executes outbound OAuth HTTP requests with SSRF validation, disabled redirects, DNS-rebinding
 * protection via IP-pinned sockets, and bounded response reading.
 */
public final class OAuthHttpClient {
  public static final int DEFAULT_MAX_RESPONSE_BYTES = 1024 * 1024;
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
   * Validates the request URL, pins the connection to validated resolved IP addresses, disables HTTP
   * redirects, and executes the request.
   *
   * @param request the outbound HTTP request
   * @param allowedHosts optional restrictive host allowlist
   * @param readBodyOnSuccess if true, reads and bounds 2xx response bodies; non-2xx bodies are never buffered
   */
  public Response execute(HttpRequest request, OAuthUrlValidator.AllowedHosts allowedHosts,
                          boolean readBodyOnSuccess) throws IOException {
    OAuthUrlValidator.ValidatedUrl validatedUrl = urlValidator.validate(
        request.getURL().toString(), OAuthUrlValidator.HostResolution.REQUIRE_RESOLVABLE, allowedHosts);
    List<InetAddress> addresses = validatedUrl.getResolvedAddresses();
    IOException lastConnectFailure = null;
    for (InetAddress address : addresses) {
      AtomicBoolean connected = new AtomicBoolean(false);
      try {
        return executePinned(request, validatedUrl, address, connected, readBodyOnSuccess);
      } catch (IOException e) {
        if (connected.get()) {
          throw e;
        }
        lastConnectFailure = e;
      }
    }
    throw lastConnectFailure != null ? lastConnectFailure : new IOException("No resolved IP addresses available");
  }

  private Response executePinned(HttpRequest request, OAuthUrlValidator.ValidatedUrl validatedUrl,
                                 InetAddress pinnedAddress, AtomicBoolean connected,
                                 boolean readBodyOnSuccess) throws IOException {
    URL targetUrl = validatedUrl.getUrl();
    int effectivePort = validatedUrl.getEffectivePort();
    HttpURLConnection connection;
    boolean isHttps = "https".equalsIgnoreCase(targetUrl.getProtocol());
    if (isHttps) {
      HttpsURLConnection httpsConnection = (HttpsURLConnection) targetUrl.openConnection(Proxy.NO_PROXY);
      httpsConnection.setSSLSocketFactory(new PinnedSSLSocketFactory(
          sslSocketFactory, validatedUrl.getConnectHost(), pinnedAddress, effectivePort,
          connectTimeoutMs, connected));
      connection = httpsConnection;
    } else {
      URL pinnedHttpUrl = new URL("http", formatUrlHost(pinnedAddress), effectivePort, targetUrl.getFile());
      connection = (HttpURLConnection) pinnedHttpUrl.openConnection(Proxy.NO_PROXY);
    }

    try {
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod(request.getMethod().name());
      connection.setConnectTimeout(connectTimeoutMs);
      connection.setReadTimeout(readTimeoutMs);
      connection.setRequestProperty("Connection", "close");

      if (request.getHeaders() != null) {
        for (Map.Entry<String, String> header : request.getHeaders().entries()) {
          if (!"Connection".equalsIgnoreCase(header.getKey())) {
            connection.addRequestProperty(header.getKey(), header.getValue());
          }
        }
      }

      ContentProvider<? extends InputStream> body = request.getBody();
      byte[] bodyBytes = null;
      if (body != null) {
        try (InputStream input = body.getInput()) {
          bodyBytes = ByteStreams.toByteArray(input);
        }
        connection.setDoOutput(true);
        // Fixed-length streaming mode stops HttpURLConnection from silently resending the request
        // (sun.net.http.retryPost) when the connection drops, which would replay single-use codes and tokens.
        connection.setFixedLengthStreamingMode(bodyBytes.length);
      }
      if (!isHttps) {
        connection.connect();
        connected.set(true);
      }
      if (bodyBytes != null) {
        try (OutputStream output = connection.getOutputStream()) {
          output.write(bodyBytes);
        }
      }

      int responseCode = connection.getResponseCode();
      connected.set(true);
      String responseMessage = connection.getResponseMessage();
      String responseBody = "";
      if (readBodyOnSuccess && responseCode >= 200 && responseCode < 300) {
        InputStream inputStream = connection.getInputStream();
        if (inputStream != null) {
          try (InputStream stream = inputStream) {
            responseBody = readBoundedUtf8(stream, connection.getContentEncoding(), maxResponseBytes);
          }
        }
      } else {
        closeQuietly(responseCode >= 200 && responseCode < 300
                         ? connection.getInputStream() : connection.getErrorStream());
      }
      return new Response(responseCode, responseMessage, responseBody);
    } finally {
      connection.disconnect();
    }
  }

  private static String formatUrlHost(InetAddress address) {
    String hostAddress = address.getHostAddress();
    if (address instanceof Inet6Address) {
      int scopeIdx = hostAddress.indexOf('%');
      String bare = scopeIdx >= 0 ? hostAddress.substring(0, scopeIdx) : hostAddress;
      return "[" + bare + "]";
    }
    return hostAddress;
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

  private static String readBoundedUtf8(InputStream rawStream, @Nullable String contentEncoding,
                                        int maxBytes) throws IOException {
    InputStream boundedRaw = new BoundedInputStream(rawStream, maxBytes);
    try (InputStream decodedStream = wrapContentEncoding(boundedRaw, contentEncoding)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[READ_BUFFER_BYTES];
      int total = 0;
      int read;
      while ((read = decodedStream.read(buffer)) != -1) {
        total += read;
        if (total > maxBytes) {
          throw new IOException("OAuth response body exceeds maximum size of " + maxBytes + " bytes");
        }
        out.write(buffer, 0, read);
      }
      return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
  }

  private static InputStream wrapContentEncoding(InputStream stream, @Nullable String contentEncoding)
      throws IOException {
    if (contentEncoding == null || contentEncoding.trim().isEmpty()) {
      return stream;
    }
    String encoding = contentEncoding.trim().toLowerCase(Locale.ROOT);
    if ("gzip".equals(encoding) || "x-gzip".equals(encoding)) {
      return new GZIPInputStream(stream);
    }
    if ("deflate".equals(encoding)) {
      return new InflaterInputStream(stream);
    }
    return stream;
  }

  /**
   * InputStream wrapper that enforces a strict byte cap before decompression or buffering.
   */
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
        bytesRead++;
        if (bytesRead > maxBytes) {
          throw new IOException("OAuth response body exceeds maximum size of " + maxBytes + " bytes");
        }
      }
      return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int read = super.read(b, off, len);
      if (read > 0) {
        bytesRead += read;
        if (bytesRead > maxBytes) {
          throw new IOException("OAuth response body exceeds maximum size of " + maxBytes + " bytes");
        }
      }
      return read;
    }
  }

  /**
   * Plain socket that always connects to the pre-validated IP address, ignoring any DNS re-resolution
   * performed by {@link HttpURLConnection}.
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
   * {@link SSLSocketFactory} that pins TCP connections strictly to a validated {@link InetAddress} while
   * preserving TLS SNI and hostname verification for the original hostname.
   */
  private static final class PinnedSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;
    private final String expectedHost;
    private final InetAddress pinnedAddress;
    private final int pinnedPort;
    private final int connectTimeoutMs;
    private final AtomicBoolean connected;

    private PinnedSSLSocketFactory(SSLSocketFactory delegate, String expectedHost,
                                   InetAddress pinnedAddress, int pinnedPort,
                                   int connectTimeoutMs, AtomicBoolean connected) {
      this.delegate = delegate;
      this.expectedHost = expectedHost;
      this.pinnedAddress = pinnedAddress;
      this.pinnedPort = pinnedPort;
      this.connectTimeoutMs = connectTimeoutMs;
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
    public Socket createSocket(String host, int port) throws IOException {
      return createPinnedSslSocket(null, 0);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
      return createPinnedSslSocket(localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      return createPinnedSslSocket(null, 0);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
        throws IOException {
      return createPinnedSslSocket(localAddress, localPort);
    }

    private Socket createPinnedSslSocket(@Nullable InetAddress localAddress, int localPort) throws IOException {
      Socket plainSocket = new Socket();
      try {
        if (localAddress != null) {
          plainSocket.bind(new InetSocketAddress(localAddress, localPort));
        }
        plainSocket.connect(new InetSocketAddress(pinnedAddress, pinnedPort), connectTimeoutMs);
        connected.set(true);
        return delegate.createSocket(plainSocket, expectedHost, pinnedPort, true);
      } catch (IOException | RuntimeException e) {
        try {
          plainSocket.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
        throw e;
      }
    }
  }
}
