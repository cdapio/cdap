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

import com.google.common.base.Strings;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DeflaterOutputStream;
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
    InetAddress primary = InetAddress.getByName("127.0.0.1");
    AtomicInteger primaryHits = new AtomicInteger();
    AtomicInteger reboundHits = new AtomicInteger();
    AtomicReference<String> capturedSni = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
    AtomicReference<String> capturedBody = new AtomicReference<>();

    try (MiniTlsServer primaryServer = new MiniTlsServer(sslContext, primary,
                                                         (sni, path, headers, body, out) -> {
                                                           primaryHits.incrementAndGet();
                                                           capturedSni.set(sni);
                                                           capturedHeaders.set(headers);
                                                           capturedBody.set(body);
                                                           writeHttpResponse(out, 200, "OK",
                                                                             "{\"access_token\":\"tok\"}", null);
                                                         })) {
      int port = primaryServer.getPort();
      InetAddress rebound = secondaryLoopbackOrSkip(port);
      // The rebound server listens on the same port, so a second DNS lookup would reach it.
      try (MiniTlsServer reboundServer = new MiniTlsServer(sslContext, rebound, port,
                                                           (sni, path, headers, body, out) -> {
                                                             reboundHits.incrementAndGet();
                                                             writeHttpResponse(out, 200, "OK",
                                                                               "{\"access_token\":\"evil\"}", null);
                                                           })) {
        // Simulate DNS rebinding: the first lookup returns the primary server, later lookups the rebound one
        AtomicInteger resolveCalls = new AtomicInteger();
        OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host -> {
          Assert.assertEquals(TEST_HOST, host);
          return new InetAddress[] {resolveCalls.getAndIncrement() == 0 ? primary : rebound};
        });
        OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);

        HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + port + "/token"))
            .addHeader("Authorization", "Basic dXNlcjpwYXNz")
            .addHeader("Content-Type", "application/x-www-form-urlencoded")
            .addHeader("User-Agent", "cdap-oauth-test")
            .withBody("grant_type=refresh_token&refresh_token=abc")
            .build();

        OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.parse(TEST_HOST), true);
        Assert.assertEquals(200, resp.getResponseCode());
        Assert.assertEquals("{\"access_token\":\"tok\"}", resp.getResponseBodyAsString());
        Assert.assertEquals(1, primaryHits.get());
        Assert.assertEquals(0, reboundHits.get());
        Assert.assertEquals(1, resolveCalls.get());
        Assert.assertEquals(TEST_HOST, capturedSni.get());
        Assert.assertEquals("Basic dXNlcjpwYXNz", capturedHeaders.get().get("authorization"));
        Assert.assertEquals("application/x-www-form-urlencoded", capturedHeaders.get().get("content-type"));
        Assert.assertEquals("cdap-oauth-test", capturedHeaders.get().get("user-agent"));
        Assert.assertEquals(TEST_HOST + ":" + port, capturedHeaders.get().get("host"));
        Assert.assertEquals("grant_type=refresh_token&refresh_token=abc", capturedBody.get());
      }
    }
  }

  @Test
  public void testPinnedHttpsIgnoresOperatingSystemDns() throws Exception {
    // "localhost" resolves through the OS. The validator pins it to a different loopback address, and the
    // client must connect to the pinned address only.
    String host = "localhost";
    SSLContext sslContext = createTestSslContext(host);
    InetAddress osDnsTarget = InetAddress.getByName(host);
    AtomicInteger osDnsHits = new AtomicInteger();
    AtomicInteger pinnedHits = new AtomicInteger();

    try (MiniTlsServer osDnsServer = new MiniTlsServer(sslContext, osDnsTarget,
                                                       (sni, path, headers, body, out) -> {
                                                         osDnsHits.incrementAndGet();
                                                         writeHttpResponse(out, 200, "OK",
                                                                           "{\"access_token\":\"os_dns\"}", null);
                                                       })) {
      int port = osDnsServer.getPort();
      InetAddress pinned = secondaryLoopbackOrSkip(port, osDnsTarget);
      try (MiniTlsServer pinnedServer = new MiniTlsServer(sslContext, pinned, port,
                                                          (sni, path, headers, body, out) -> {
                                                            pinnedHits.incrementAndGet();
                                                            writeHttpResponse(out, 200, "OK",
                                                                              "{\"access_token\":\"pinned\"}", null);
                                                          })) {
        OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, h -> new InetAddress[] {pinned});
        OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, 1024);
        HttpRequest req = HttpRequest.post(new URL("https://" + host + ":" + port + "/token"))
            .withBody("grant_type=refresh_token&refresh_token=abc")
            .build();

        OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.allowAll(), true);
        Assert.assertEquals("{\"access_token\":\"pinned\"}", resp.getResponseBodyAsString());
        Assert.assertEquals(1, pinnedHits.get());
        Assert.assertEquals(0, osDnsHits.get());
      }
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
      // The first address has no listener on the port, so connect fails and the client falls back
      InetAddress refusing = secondaryLoopbackOrSkip(server.getPort());
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host -> new InetAddress[] {
          refusing,
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
  public void testConnectAttemptsAreCapped() throws Exception {
    InetAddress[] refusing = new InetAddress[OAuthHttpClient.MAX_CONNECT_ATTEMPTS];
    for (int i = 0; i < refusing.length; i++) {
      refusing[i] = InetAddress.getByName("127.0.0." + (i + 2));
    }
    InetAddress listening = InetAddress.getByName("127.0.0." + (refusing.length + 2));
    Assume.assumeTrue("Host cannot bind " + listening, canBind(listening));
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    AtomicInteger serverHits = new AtomicInteger();

    try (MiniTlsServer server = new MiniTlsServer(sslContext, listening,
                                                  (sni, path, headers, body, out) -> {
                                                    serverHits.incrementAndGet();
                                                    writeHttpResponse(out, 200, "OK", "{}", null);
                                                  })) {
      URL url = new URL("https://" + TEST_HOST + ":" + server.getPort() + "/token");

      // The listening address is the 5th record, beyond the attempt limit, so it is never tried
      InetAddress[] tooMany = Arrays.copyOf(refusing, refusing.length + 1);
      tooMany[refusing.length] = listening;
      OAuthHttpClient client = new OAuthHttpClient(
          OAuthUrlValidator.forTesting(true, true, host -> tooMany), sslContext.getSocketFactory(), 5000, 5000, 1024);
      try {
        client.execute(HttpRequest.get(url).build(), AllowedHosts.allowAll(), true);
        Assert.fail("Expected ConnectException when the reachable address is beyond the attempt limit");
      } catch (ConnectException expected) {
        // expected
      }
      Assert.assertEquals(0, serverHits.get());

      // Within the limit, the client reaches the listening address after the refused ones
      InetAddress[] withinLimit = Arrays.copyOf(refusing, refusing.length);
      withinLimit[refusing.length - 1] = listening;
      client = new OAuthHttpClient(
          OAuthUrlValidator.forTesting(true, true, host -> withinLimit), sslContext.getSocketFactory(),
          5000, 5000, 1024);
      Assert.assertEquals(200, client.execute(HttpRequest.get(url).build(), AllowedHosts.allowAll(), true)
          .getResponseCode());
      Assert.assertEquals(1, serverHits.get());
    }
  }

  @Test
  public void testSelectConnectCandidatesInterleavesFamiliesAndCaps() throws Exception {
    List<InetAddress> ipv6 = new ArrayList<>();
    for (int i = 1; i <= 7; i++) {
      ipv6.add(InetAddress.getByName("2001:db8::" + i));
    }
    InetAddress v4a = InetAddress.getByName("192.0.2.1");
    InetAddress v4b = InetAddress.getByName("192.0.2.2");
    List<InetAddress> dualStack = new ArrayList<>(ipv6);
    dualStack.add(v4a);
    dualStack.add(v4b);

    // Seven IPv6 records ahead of two IPv4 records must not use up every attempt on IPv6
    Assert.assertEquals(Arrays.asList(ipv6.get(0), v4a, ipv6.get(1), v4b),
                        OAuthHttpClient.selectConnectCandidates(dualStack, 4));
    Assert.assertEquals(ipv6.subList(0, 4), OAuthHttpClient.selectConnectCandidates(ipv6, 4));
    Assert.assertEquals(Arrays.asList(v4a, v4b), OAuthHttpClient.selectConnectCandidates(Arrays.asList(v4a, v4b), 4));
    Assert.assertTrue(OAuthHttpClient.selectConnectCandidates(new ArrayList<InetAddress>(), 4).isEmpty());
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

  @Test
  public void testDeflateDecodingAndExactResponseSizeLimit() throws Exception {
    SSLContext sslContext = createTestSslContext(TEST_HOST);
    int limit = 512;
    String exactPayload = Strings.repeat("x", limit);
    String oneByteOver = exactPayload + "y";
    String oversized = Strings.repeat("0123456789", 400);

    try (MiniTlsServer server = new MiniTlsServer(sslContext, InetAddress.getByName("127.0.0.1"),
                                                  (sni, path, headers, body, out) -> {
                                                    if ("/deflate-ok".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK",
                                                                        "{\"access_token\":\"def\"}", "deflate");
                                                    } else if ("/deflate-bomb".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK", oversized, "deflate");
                                                    } else if ("/exact".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK", exactPayload, null);
                                                    } else if ("/exact-gzip".equals(path)) {
                                                      writeHttpResponse(out, 200, "OK", exactPayload, "gzip");
                                                    } else {
                                                      writeHttpResponse(out, 200, "OK", oneByteOver, null);
                                                    }
                                                  })) {
      OAuthUrlValidator validator = OAuthUrlValidator.forTesting(true, true, host ->
          new InetAddress[] {InetAddress.getByName("127.0.0.1")});
      OAuthHttpClient client = new OAuthHttpClient(validator, sslContext.getSocketFactory(), 5000, 5000, limit);
      String base = "https://" + TEST_HOST + ":" + server.getPort();

      Assert.assertEquals("{\"access_token\":\"def\"}", client.execute(
          HttpRequest.get(new URL(base + "/deflate-ok")).build(), AllowedHosts.allowAll(), true)
          .getResponseBodyAsString());
      // A body of exactly the limit is accepted, both raw and after decompression
      Assert.assertEquals(exactPayload, client.execute(
          HttpRequest.get(new URL(base + "/exact")).build(), AllowedHosts.allowAll(), true).getResponseBodyAsString());
      Assert.assertEquals(exactPayload, client.execute(
          HttpRequest.get(new URL(base + "/exact-gzip")).build(), AllowedHosts.allowAll(), true)
          .getResponseBodyAsString());

      for (String path : new String[] {"/deflate-bomb", "/over"}) {
        try {
          client.execute(HttpRequest.get(new URL(base + path)).build(), AllowedHosts.allowAll(), true);
          Assert.fail("Expected IOException for response larger than the limit: " + path);
        } catch (IOException expected) {
          Assert.assertTrue(expected.getMessage().contains("exceeds maximum size"));
        }
      }
    }
  }

  private static void writeHttpResponse(OutputStream out, int code, String reason,
                                        String body, String contentEncoding) throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    if ("deflate".equals(contentEncoding)) {
      ByteArrayOutputStream deflated = new ByteArrayOutputStream();
      try (DeflaterOutputStream deflater = new DeflaterOutputStream(deflated)) {
        deflater.write(payload);
      }
      payload = deflated.toByteArray();
    }
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
    return secondaryLoopbackOrSkip(port, InetAddress.getByName("127.0.0.1"));
  }

  private static InetAddress secondaryLoopbackOrSkip(int port, InetAddress exclude) throws IOException {
    for (String candidate : new String[] {"127.0.0.2", "::1", "127.0.0.1"}) {
      InetAddress address = InetAddress.getByName(candidate);
      if (address.equals(exclude)) {
        continue;
      }
      try (ServerSocket probe = new ServerSocket(port, 1, address)) {
        return address;
      } catch (IOException e) {
        // Try the next candidate
      }
    }
    Assume.assumeTrue("No secondary loopback address can bind port " + port, false);
    throw new AssertionError("unreachable");
  }

  private static boolean canBind(InetAddress address) {
    try (ServerSocket probe = new ServerSocket(0, 1, address)) {
      return true;
    } catch (IOException e) {
      return false;
    }
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
