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

import io.cdap.cdap.datapipeline.oauth.OAuthUrlValidator.AllowedHosts;
import io.cdap.common.http.HttpRequest;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Unit tests for {@link OAuthHttpClient}, including real TLS IP pinning, SNI/hostname verification,
 * DNS rebinding immunity, multi-IP fallback, redirect blocking, gzip decoding, and response bounds.
 */
public class OAuthHttpClientTest {
  private static final String TEST_HOST = "oauth-pinned.invalid";

  @Test
  public void testPinnedHttpsPreservesSniHeadersBodyAndResistsDnsRebinding() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    AtomicInteger serverAHits = new AtomicInteger();
    AtomicInteger serverBHits = new AtomicInteger();
    AtomicReference<String> capturedSni = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
    AtomicReference<String> capturedBody = new AtomicReference<>();

    try (MiniTlsServer serverA = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                   (sni, path, headers, body, out) -> {
                                                     serverAHits.incrementAndGet();
                                                     capturedSni.set(sni);
                                                     capturedHeaders.set(headers);
                                                     capturedBody.set(body);
                                                     writeHttpResponse(out, 200, "OK",
                                                                       "{\"access_token\":\"tok\"}", null);
                                                   });
         MiniTlsServer serverB = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.2"),
                                                   (sni, path, headers, body, out) -> {
                                                     serverBHits.incrementAndGet();
                                                     writeHttpResponse(out, 200, "OK",
                                                                       "{\"access_token\":\"evil\"}", null);
                                                   })) {
      // Simulate DNS rebinding: first resolution returns 127.0.0.1 (Server A), subsequent calls return 127.0.0.2
      AtomicInteger resolveCalls = new AtomicInteger();
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host -> {
        Assert.assertEquals(TEST_HOST, host);
        if (resolveCalls.getAndIncrement() == 0) {
          return new InetAddress[] {InetAddress.getByName("127.0.0.1")};
        }
        return new InetAddress[] {InetAddress.getByName("127.0.0.2")};
      });
      OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);

      HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + serverA.getPort() + "/token"))
          .addHeader("Authorization", "Basic dXNlcjpwYXNz")
          .addHeader("Content-Type", "application/x-www-form-urlencoded")
          .addHeader("User-Agent", "cdap-oauth-test")
          .withBody("grant_type=refresh_token&refresh_token=abc")
          .build();

      OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.parse(TEST_HOST), true);
      Assert.assertEquals(200, resp.getResponseCode());
      Assert.assertEquals("{\"access_token\":\"tok\"}", resp.getResponseBodyAsString());
      Assert.assertEquals(1, serverAHits.get());
      Assert.assertEquals(0, serverBHits.get());
      Assert.assertEquals(1, resolveCalls.get());
      Assert.assertEquals(TEST_HOST, capturedSni.get());
      Assert.assertEquals("Basic dXNlcjpwYXNz", capturedHeaders.get().get("authorization"));
      Assert.assertEquals("application/x-www-form-urlencoded", capturedHeaders.get().get("content-type"));
      Assert.assertEquals("cdap-oauth-test", capturedHeaders.get().get("user-agent"));
      Assert.assertEquals(TEST_HOST + ":" + serverA.getPort(), capturedHeaders.get().get("host"));
      Assert.assertEquals("grant_type=refresh_token&refresh_token=abc", capturedBody.get());
    }
  }

  @Test
  public void testMultiRecordFallbackRetriesOnlyBeforeTcpConnect() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    AtomicInteger serverHits = new AtomicInteger();

    try (MiniTlsServer server = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                  (sni, path, headers, body, out) -> {
                                                    serverHits.incrementAndGet();
                                                    writeHttpResponse(out, 200, "OK",
                                                                      "{\"access_token\":\"fallback_ok\"}", null);
                                                  })) {
      // First IP (127.0.0.2) has no listener on server.getPort() so connect fails; fallback to 127.0.0.1 succeeds
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host -> new InetAddress[] {
          InetAddress.getByName("127.0.0.2"),
          InetAddress.getByName("127.0.0.1")
      });
      OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);

      HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/token"))
          .withBody("grant_type=refresh_token")
          .build();
      OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.allowAll(), true);
      Assert.assertEquals(200, resp.getResponseCode());
      Assert.assertEquals("{\"access_token\":\"fallback_ok\"}", resp.getResponseBodyAsString());
      Assert.assertEquals(1, serverHits.get());
    }
  }

  @Test
  public void testRedirectsAreNotFollowed() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    AtomicInteger secretHits = new AtomicInteger();

    try (MiniTlsServer server = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                  (sni, path, headers, body, out) -> {
                                                    if ("/secret".equals(path)) {
                                                      secretHits.incrementAndGet();
                                                      writeHttpResponse(out, 200, "OK", "SECRET", null);
                                                      return;
                                                    }
                                                    String location = "https://" + TEST_HOST + "/secret";
                                                    String raw = "HTTP/1.1 302 Found\r\n"
                                                        + "Location: " + location + "\r\n"
                                                        + "Content-Length: 0\r\n"
                                                        + "Connection: close\r\n\r\n";
                                                    out.write(raw.getBytes(StandardCharsets.US_ASCII));
                                                  })) {
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
          new InetAddress[] {InetAddress.getByName("127.0.0.1")});
      OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);

      HttpRequest req = HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/redirect")).build();
      OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.allowAll(), true);
      Assert.assertEquals(302, resp.getResponseCode());
      Assert.assertEquals("", resp.getResponseBodyAsString());
      Assert.assertEquals(0, secretHits.get());
    }
  }

  @Test
  public void testGzipDecodingResponseBoundsAndDiscardedBodies() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    StringBuilder bigBuilder = new StringBuilder();
    for (int i = 0; i < 400; i++) {
      bigBuilder.append("0123456789");
    }
    String oversizedPayload = bigBuilder.toString();

    try (MiniTlsServer server = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                  (sni, path, headers, body, out) -> {
                                                    if ("/gzip-ok".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK",
                                                                        "{\"access_token\":\"gz\"}", "gzip");
                                                    } else if ("/gzip-bomb".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK", oversizedPayload, "gzip");
                                                    } else if ("/plain-big".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK", oversizedPayload, null);
                                                    } else {
                                                      writeHttpResponse(out, 400, "Bad Request",
                                                                        oversizedPayload, null);
                                                    }
                                                  })) {
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
          new InetAddress[] {InetAddress.getByName("127.0.0.1")});
      OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 512);

      // Valid gzip response under 512 bytes decodes cleanly
      OAuthHttpClient.Response gzResp = client.execute(
          HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/gzip-ok")).build(),
          AllowedHosts.allowAll(), true);
      Assert.assertEquals(200, gzResp.getResponseCode());
      Assert.assertEquals("{\"access_token\":\"gz\"}", gzResp.getResponseBodyAsString());

      // Oversized decompressed gzip response throws IOException
      try {
        client.execute(
            HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/gzip-bomb")).build(),
            AllowedHosts.allowAll(), true);
        Assert.fail("Expected IOException for oversized gzip response");
      } catch (IOException expected) {
        Assert.assertTrue(expected.getMessage().contains("exceeds maximum size"));
      }

      // Oversized plain 200 throws IOException when readBodyOnSuccess=true
      try {
        client.execute(
            HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/plain-big")).build(),
            AllowedHosts.allowAll(), true);
        Assert.fail("Expected IOException for oversized plain response");
      } catch (IOException expected) {
        Assert.assertTrue(expected.getMessage().contains("exceeds maximum size"));
      }

      // Oversized plain 200 succeeds without buffering when readBodyOnSuccess=false (identityUrl validity check)
      OAuthHttpClient.Response noBodyResp = client.execute(
          HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/plain-big")).build(),
          AllowedHosts.allowAll(), false);
      Assert.assertEquals(200, noBodyResp.getResponseCode());
      Assert.assertEquals("", noBodyResp.getResponseBodyAsString());

      // Oversized non-2xx error response is discarded cleanly without buffering
      OAuthHttpClient.Response errResp = client.execute(
          HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/err-big")).build(),
          AllowedHosts.allowAll(), true);
      Assert.assertEquals(400, errResp.getResponseCode());
      Assert.assertEquals("", errResp.getResponseBodyAsString());
    }
  }

  @Test
  public void testPinnedHttpsRejectsCertificateThatDoesNotMatchRequestHost() throws Exception {
    // The server certificate is trusted, but its SAN names the pinned IP or another host instead of
    // the request host. The client must fail the TLS identity check before sending the HTTP request.
    GeneralName[] mismatchedSans = {
        new GeneralName(GeneralName.dNSName, "wrong-host.invalid"),
        new GeneralName(GeneralName.iPAddress, "127.0.0.1")
    };
    for (GeneralName mismatchedSan : mismatchedSans) {
      SSLContext sslContext = createTestSslContext("mismatch", mismatchedSan);
      AtomicInteger serverHits = new AtomicInteger();
      try (MiniTlsServer server = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                    (sni, path, headers, body, out) -> {
                                                      serverHits.incrementAndGet();
                                                      writeHttpResponse(out, 200, "OK",
                                                                        "{\"access_token\":\"leaked\"}", null);
                                                    })) {
        OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
            new InetAddress[] {InetAddress.getByName("127.0.0.1")});
        OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);
        HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/token"))
            .addHeader("Authorization", "Basic dXNlcjpwYXNz")
            .withBody("grant_type=refresh_token&refresh_token=secret")
            .build();
        try {
          client.execute(req, AllowedHosts.allowAll(), true);
          Assert.fail("Expected TLS hostname verification failure for SAN " + mismatchedSan);
        } catch (IOException expected) {
          // SSLHandshakeException (endpoint identification) or IOException("HTTPS hostname wrong")
        }
        Assert.assertEquals("HTTP request must not be sent for SAN " + mismatchedSan, 0, serverHits.get());
      }
    }
  }

  @Test
  public void testNoRetryAfterConnectAndFailureWhenAllAddressesRefuse() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    InetAddress primary = InetAddress.getByName("127.0.0.1");
    AtomicInteger primaryHits = new AtomicInteger();
    AtomicInteger secondaryHits = new AtomicInteger();

    // 1. The first address accepts the TCP/TLS connection and the request, then drops the connection
    // without a response. The single-use code must not be sent again, either to the same address by
    // the JDK's silent POST retry or to the next resolved address.
    try (MiniTlsServer primaryServer = new MiniTlsServer(sslContext, primary,
                                                         (sni, path, headers, body, out) -> {
                                                           primaryHits.incrementAndGet();
                                                           out.close();
                                                         })) {
      int port = primaryServer.getPort();
      InetAddress secondary = secondaryLoopbackOrSkip(port);
      try (MiniTlsServer secondaryServer = new MiniTlsServer(sslContext, secondary, port,
                                                             (sni, path, headers, body, out) -> {
                                                               secondaryHits.incrementAndGet();
                                                               writeHttpResponse(out, 200, "OK",
                                                                                 "{\"access_token\":\"replayed\"}",
                                                                                 null);
                                                             })) {
        OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
            new InetAddress[] {primary, secondary});
        OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);
        HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + port + "/token"))
            .withBody("code=one_time_code&grant_type=authorization_code")
            .build();
        try {
          client.execute(req, AllowedHosts.allowAll(), true);
          Assert.fail("Expected IOException when the server drops the connection after the request");
        } catch (IOException expected) {
          // expected
        }
        Assert.assertEquals("Request must be sent exactly once", 1, primaryHits.get());
        Assert.assertEquals("Request must not fall back to another address after connect", 0, secondaryHits.get());
      }
    }

    // 2. When every resolved address refuses the TCP connection, the last connect failure is thrown.
    int unusedPort;
    try (ServerSocket probe = new ServerSocket(0, 1, primary)) {
      unusedPort = probe.getLocalPort();
    }
    InetAddress secondary = secondaryLoopbackOrSkip(unusedPort);
    OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
        new InetAddress[] {primary, secondary});
    OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 2000, 2000, 1024);
    try {
      client.execute(HttpRequest.get(new URL("https://" + TEST_HOST + ":" + unusedPort + "/token")).build(),
                     AllowedHosts.allowAll(), true);
      Assert.fail("Expected ConnectException when all resolved addresses refuse the connection");
    } catch (ConnectException expected) {
      // expected
    }
  }

  private static void writeHttpResponse(OutputStream out, int code, String reason,
                                        String body, String contentEncoding) throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    if ("gzip".equals(contentEncoding)) {
      ByteArrayOutputStream gzOut = new ByteArrayOutputStream();
      try (GZIPOutputStream gzip = new GZIPOutputStream(gzOut)) {
        gzip.write(payload);
      }
      payload = gzOut.toByteArray();
    }
    StringBuilder header = new StringBuilder();
    header.append("HTTP/1.1 ").append(code).append(' ').append(reason).append("\r\n");
    if (contentEncoding != null) {
      header.append("Content-Encoding: ").append(contentEncoding).append("\r\n");
    }
    header.append("Content-Length: ").append(payload.length).append("\r\n");
    header.append("Connection: close\r\n\r\n");
    out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
    out.write(payload);
    out.flush();
  }

  /**
   * Returns a loopback address other than 127.0.0.1 that can bind {@code port}, or skips the test when
   * the host has no second loopback address (for example macOS without a 127.0.0.2 alias and no IPv6).
   */
  private static InetAddress secondaryLoopbackOrSkip(int port) throws IOException {
    for (String candidate : new String[] {"127.0.0.2", "::1"}) {
      InetAddress address = InetAddress.getByName(candidate);
      try (ServerSocket probe = new ServerSocket(port, 1, address)) {
        return address;
      } catch (IOException e) {
        // Try the next candidate
      }
    }
    Assume.assumeTrue("No secondary loopback address can bind port " + port, false);
    throw new AssertionError("unreachable");
  }

  private static SSLContext createTestSslContext(String dnsSan) throws Exception {
    return createTestSslContext(dnsSan, new GeneralName(GeneralName.dNSName, dnsSan));
  }

  private static SSLContext createTestSslContext(String commonName, GeneralName subjectAltName)
      throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048, new SecureRandom());
    KeyPair keyPair = keyGen.generateKeyPair();

    long now = System.currentTimeMillis();
    X500Name dn = new X500Name("CN=" + commonName);
    JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        dn,
        BigInteger.valueOf(now),
        new Date(now - TimeUnit.HOURS.toMillis(1)),
        new Date(now + TimeUnit.DAYS.toMillis(7)),
        dn,
        keyPair.getPublic());
    GeneralNames subjectAltNames = new GeneralNames(subjectAltName);
    certBuilder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);

    ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA")
        .setProvider(new BouncyCastleProvider())
        .build(keyPair.getPrivate());
    X509CertificateHolder holder = certBuilder.build(signer);
    X509Certificate cert = new JcaX509CertificateConverter()
        .setProvider(new BouncyCastleProvider())
        .getCertificate(holder);

    char[] pass = "changeit".toCharArray();
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(null, pass);
    ks.setKeyEntry("server", keyPair.getPrivate(), pass, new Certificate[] {cert});

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, pass);

    KeyStore ts = KeyStore.getInstance("JKS");
    ts.load(null, pass);
    ts.setCertificateEntry("server", cert);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);

    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return ctx;
  }

  @FunctionalInterface
  private interface RequestHandler {
    void handle(String sniHost, String path, Map<String, String> headers, String body,
                OutputStream out) throws IOException;
  }

  private static final class MiniTlsServer implements AutoCloseable {
    private final SSLServerSocket serverSocket;
    private final Thread acceptThread;

    private MiniTlsServer(SSLContext sslContext, InetAddress bindAddress, RequestHandler handler) throws IOException {
      this(sslContext, bindAddress, 0, handler);
    }

    private MiniTlsServer(SSLContext sslContext, InetAddress bindAddress, int port,
                          RequestHandler handler) throws IOException {
      this.serverSocket = (SSLServerSocket) sslContext.getServerSocketFactory()
          .createServerSocket(port, 16, bindAddress);
      this.acceptThread = new Thread(() -> {
        while (!serverSocket.isClosed()) {
          try (SSLSocket socket = (SSLSocket) serverSocket.accept()) {
            socket.setSoTimeout(5000);
            socket.startHandshake();
            String sniHost = null;
            if (socket.getSession() instanceof ExtendedSSLSession) {
              List<SNIServerName> names = ((ExtendedSSLSession) socket.getSession()).getRequestedServerNames();
              for (SNIServerName name : names) {
                if (name instanceof SNIHostName) {
                  sniHost = ((SNIHostName) name).getAsciiName();
                }
              }
            }
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            String requestLine = reader.readLine();
            if (requestLine == null) {
              continue;
            }
            String[] parts = requestLine.split(" ");
            String path = parts.length > 1 ? parts[1] : "/";
            Map<String, String> headers = new HashMap<>();
            String line;
            int contentLength = 0;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
              int idx = line.indexOf(':');
              if (idx > 0) {
                String k = line.substring(0, idx).trim().toLowerCase(Locale.ROOT);
                String v = line.substring(idx + 1).trim();
                headers.put(k, v);
                if ("content-length".equals(k)) {
                  contentLength = Integer.parseInt(v);
                }
              }
            }
            char[] bodyChars = new char[contentLength];
            int offset = 0;
            while (offset < contentLength) {
              int r = reader.read(bodyChars, offset, contentLength - offset);
              if (r == -1) {
                break;
              }
              offset += r;
            }
            handler.handle(sniHost, path, headers, new String(bodyChars, 0, offset), socket.getOutputStream());
          } catch (IOException ignored) {
            // Expected on shutdown
          }
        }
      });
      this.acceptThread.setDaemon(true);
      this.acceptThread.start();
    }

    int getPort() {
      return serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      try {
        acceptThread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
