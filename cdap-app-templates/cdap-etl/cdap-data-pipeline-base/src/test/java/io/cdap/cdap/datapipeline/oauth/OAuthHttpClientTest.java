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
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link OAuthHttpClient}: real TLS IP pinning, SNI and cert hostname verification,
 * DNS rebinding immunity, multi-IP fallback, redirect blocking, gzip decoding, and response bounds.
 */
public class OAuthHttpClientTest {
  private static final String TEST_HOST = "oauth-pinned.invalid";
  private static InetAddress loopback;
  private static SSLContext testSslContext;

  @BeforeClass
  public static void setUpClass() throws Exception {
    loopback = InetAddress.getByName("127.0.0.1");
    testSslContext = createTestSslContext(TEST_HOST, new GeneralName(GeneralName.dNSName, TEST_HOST));
  }

  private static OAuthHttpClient newClient(SSLContext ssl, int maxBytes, OAuthUrlValidator.DnsResolver resolver) {
    return new OAuthHttpClient(OAuthUrlValidator.forTesting(true, true, resolver),
                               ssl.getSocketFactory(), 5000, 5000, maxBytes);
  }

  private static OAuthHttpClient newClient(InetAddress... ips) {
    return newClient(testSslContext, 1024, host -> ips);
  }

  @Test
  public void testPinnedHttpsPreservesSniHeadersBodyAndResistsDnsRebinding() throws Exception {
    AtomicInteger primaryHits = new AtomicInteger();
    AtomicInteger reboundHits = new AtomicInteger();
    AtomicReference<String> capturedSni = new AtomicReference<>();
    AtomicReference<Map<String, String>> capturedHeaders = new AtomicReference<>();
    AtomicReference<String> capturedBody = new AtomicReference<>();

    try (MiniTlsServer primaryServer = new MiniTlsServer(testSslContext, loopback, 0,
        (sni, path, headers, body, out) -> {
          primaryHits.incrementAndGet();
          capturedSni.set(sni);
          capturedHeaders.set(headers);
          capturedBody.set(body);
          writeHttpResponse(out, 200, "OK", "{\"access_token\":\"tok\"}", false);
        })) {
      int port = primaryServer.getPort();
      InetAddress rebound = secondaryLoopbackOrSkip(port, loopback);
      // Same port on a second IP so any second DNS lookup would hit reboundServer.
      try (MiniTlsServer reboundServer = new MiniTlsServer(testSslContext, rebound, port,
          (sni, path, headers, body, out) -> {
            reboundHits.incrementAndGet();
            writeHttpResponse(out, 200, "OK", "{\"access_token\":\"evil\"}", false);
          })) {
        AtomicInteger resolveCalls = new AtomicInteger();
        OAuthHttpClient client = newClient(testSslContext, 1024, host -> {
          Assert.assertEquals(TEST_HOST, host);
          return new InetAddress[] {resolveCalls.getAndIncrement() == 0 ? loopback : rebound};
        });

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
        Assert.assertEquals("identity", capturedHeaders.get().get("accept-encoding"));
        Assert.assertEquals("grant_type=refresh_token&refresh_token=abc", capturedBody.get());
      }
    }
  }

  @Test
  public void testPinnedHttpsIgnoresOperatingSystemDns() throws Exception {
    SSLContext localhostSsl = createTestSslContext("localhost", new GeneralName(GeneralName.dNSName, "localhost"));
    InetAddress osDnsTarget = InetAddress.getByName("localhost");
    AtomicInteger osDnsHits = new AtomicInteger();
    AtomicInteger pinnedHits = new AtomicInteger();

    try (MiniTlsServer osDnsServer = new MiniTlsServer(localhostSsl, osDnsTarget, 0,
        (sni, path, headers, body, out) -> {
          osDnsHits.incrementAndGet();
          writeHttpResponse(out, 200, "OK", "{\"access_token\":\"os_dns\"}", false);
        })) {
      int port = osDnsServer.getPort();
      InetAddress pinned = secondaryLoopbackOrSkip(port, osDnsTarget);
      try (MiniTlsServer pinnedServer = new MiniTlsServer(localhostSsl, pinned, port,
          (sni, path, headers, body, out) -> {
            pinnedHits.incrementAndGet();
            writeHttpResponse(out, 200, "OK", "{\"access_token\":\"pinned\"}", false);
          })) {
        OAuthHttpClient client = newClient(localhostSsl, 1024, h -> new InetAddress[] {pinned});
        HttpRequest req = HttpRequest.post(new URL("https://localhost:" + port + "/token"))
            .withBody("grant_type=refresh_token&refresh_token=abc").build();

        OAuthHttpClient.Response resp = client.execute(req, AllowedHosts.allowAll(), true);
        Assert.assertEquals("{\"access_token\":\"pinned\"}", resp.getResponseBodyAsString());
        Assert.assertEquals(1, pinnedHits.get());
        Assert.assertEquals(0, osDnsHits.get());
      }
    }
  }

  @Test
  public void testMultiRecordFallbackCapAndFamilyInterleaving() throws Exception {
    // 1. Interleaving ensures IPv6 records ahead of IPv4 records cannot exhaust all attempts.
    List<InetAddress> ipv6 = new ArrayList<>();
    for (int i = 1; i <= 7; i++) {
      ipv6.add(InetAddress.getByName("2001:db8::" + i));
    }
    InetAddress v4a = InetAddress.getByName("192.0.2.1");
    InetAddress v4b = InetAddress.getByName("192.0.2.2");
    List<InetAddress> dualStack = new ArrayList<>(ipv6);
    dualStack.add(v4a);
    dualStack.add(v4b);
    Assert.assertEquals(Arrays.asList(ipv6.get(0), v4a, ipv6.get(1), v4b),
                        OAuthHttpClient.selectConnectCandidates(dualStack, 4));
    Assert.assertEquals(ipv6.subList(0, 4), OAuthHttpClient.selectConnectCandidates(ipv6, 4));
    Assert.assertTrue(OAuthHttpClient.selectConnectCandidates(new ArrayList<InetAddress>(), 4).isEmpty());

    // 2. Live fallback within MAX_CONNECT_ATTEMPTS succeeds; beyond the cap fails without hitting the server.
    InetAddress[] refusing = new InetAddress[OAuthHttpClient.MAX_CONNECT_ATTEMPTS];
    for (int i = 0; i < refusing.length; i++) {
      refusing[i] = InetAddress.getByName("127.0.0." + (i + 2));
    }
    InetAddress listening = InetAddress.getByName("127.0.0." + (refusing.length + 2));
    Assume.assumeTrue("Host cannot bind " + listening, canBind(listening));
    AtomicInteger serverHits = new AtomicInteger();

    try (MiniTlsServer server = new MiniTlsServer(testSslContext, listening, 0,
        (sni, path, headers, body, out) -> {
          serverHits.incrementAndGet();
          writeHttpResponse(out, 200, "OK", "{\"access_token\":\"fallback_ok\"}", false);
        })) {
      URL url = new URL("https://" + TEST_HOST + ":" + server.getPort() + "/token");
      InetAddress[] tooMany = Arrays.copyOf(refusing, refusing.length + 1);
      tooMany[refusing.length] = listening;
      try {
        newClient(tooMany).execute(HttpRequest.get(url).build(), AllowedHosts.allowAll(), true);
        Assert.fail("Expected ConnectException beyond attempt limit");
      } catch (ConnectException expected) {
        // expected
      }
      Assert.assertEquals(0, serverHits.get());

      InetAddress[] withinLimit = Arrays.copyOf(refusing, refusing.length);
      withinLimit[refusing.length - 1] = listening;
      HttpRequest post = HttpRequest.post(url).withBody("grant_type=refresh_token").build();
      OAuthHttpClient.Response resp = newClient(withinLimit).execute(post, AllowedHosts.allowAll(), true);
      Assert.assertEquals(200, resp.getResponseCode());
      Assert.assertEquals("{\"access_token\":\"fallback_ok\"}", resp.getResponseBodyAsString());
      Assert.assertEquals(1, serverHits.get());
    }
  }

  @Test
  public void testRedirectsAreNotFollowed() throws Exception {
    AtomicInteger secretHits = new AtomicInteger();
    try (MiniTlsServer server = new MiniTlsServer(testSslContext, loopback, 0,
        (sni, path, headers, body, out) -> {
          if ("/secret".equals(path)) {
            secretHits.incrementAndGet();
            writeHttpResponse(out, 200, "OK", "SECRET", false);
            return;
          }
          String raw = "HTTP/1.1 302 Found\r\nLocation: https://" + TEST_HOST + "/secret\r\n"
              + "Content-Length: 0\r\nConnection: close\r\n\r\n";
          out.write(raw.getBytes(StandardCharsets.US_ASCII));
        })) {
      HttpRequest req = HttpRequest.get(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/redirect")).build();
      OAuthHttpClient.Response resp = newClient(loopback).execute(req, AllowedHosts.allowAll(), true);
      Assert.assertEquals(302, resp.getResponseCode());
      Assert.assertEquals("", resp.getResponseBodyAsString());
      Assert.assertEquals(0, secretHits.get());
    }
  }

  @Test
  public void testGzipDecodingResponseBoundsAndDiscardedBodies() throws Exception {
    int limit = 512;
    String exactPayload = Strings.repeat("x", limit);
    String oversized = Strings.repeat("0123456789", 400);

    try (MiniTlsServer server = new MiniTlsServer(testSslContext, loopback, 0,
        (sni, path, headers, body, out) -> {
          if ("/gzip-ok".equals(path)) {
            writeHttpResponse(out, 200, "OK", "{\"access_token\":\"gz\"}", true);
          } else if ("/gzip-bomb".equals(path)) {
            writeHttpResponse(out, 200, "OK", oversized, true);
          } else if ("/exact".equals(path)) {
            writeHttpResponse(out, 200, "OK", exactPayload, false);
          } else if ("/exact-gzip".equals(path)) {
            writeHttpResponse(out, 200, "OK", exactPayload, true);
          } else if ("/over".equals(path)) {
            writeHttpResponse(out, 200, "OK", exactPayload + "y", false);
          } else if ("/plain-big".equals(path)) {
            writeHttpResponse(out, 200, "OK", oversized, false);
          } else {
            writeHttpResponse(out, 400, "Bad Request", oversized, false);
          }
        })) {
      OAuthHttpClient client = newClient(testSslContext, limit, host -> new InetAddress[] {loopback});
      String base = "https://" + TEST_HOST + ":" + server.getPort();

      Assert.assertEquals("{\"access_token\":\"gz\"}",
                          client.execute(HttpRequest.get(new URL(base + "/gzip-ok")).build(),
                                         AllowedHosts.allowAll(), true).getResponseBodyAsString());
      Assert.assertEquals(exactPayload,
                          client.execute(HttpRequest.get(new URL(base + "/exact")).build(),
                                         AllowedHosts.allowAll(), true).getResponseBodyAsString());
      Assert.assertEquals(exactPayload,
                          client.execute(HttpRequest.get(new URL(base + "/exact-gzip")).build(),
                                         AllowedHosts.allowAll(), true).getResponseBodyAsString());

      for (String path : new String[] {"/gzip-bomb", "/plain-big", "/over"}) {
        try {
          client.execute(HttpRequest.get(new URL(base + path)).build(), AllowedHosts.allowAll(), true);
          Assert.fail("Expected IOException for oversized response: " + path);
        } catch (IOException expected) {
          Assert.assertTrue(expected.getMessage().contains("exceeds maximum size"));
        }
      }

      // Discarded 2xx (identityUrl) and non-2xx bodies succeed without buffering
      OAuthHttpClient.Response noBody = client.execute(
          HttpRequest.get(new URL(base + "/plain-big")).build(), AllowedHosts.allowAll(), false);
      Assert.assertEquals(200, noBody.getResponseCode());
      Assert.assertEquals("", noBody.getResponseBodyAsString());

      OAuthHttpClient.Response err = client.execute(
          HttpRequest.get(new URL(base + "/err-big")).build(), AllowedHosts.allowAll(), true);
      Assert.assertEquals(400, err.getResponseCode());
      Assert.assertEquals("", err.getResponseBodyAsString());
    }
  }

  @Test
  public void testPinnedHttpsRejectsMismatchedCertificateSan() throws Exception {
    GeneralName[] mismatchedSans = {
        new GeneralName(GeneralName.dNSName, "wrong-host.invalid"),
        new GeneralName(GeneralName.iPAddress, "127.0.0.1")
    };
    for (GeneralName mismatchedSan : mismatchedSans) {
      SSLContext mismatchSsl = createTestSslContext("mismatch", mismatchedSan);
      AtomicInteger serverHits = new AtomicInteger();
      try (MiniTlsServer server = new MiniTlsServer(mismatchSsl, loopback, 0,
          (sni, path, headers, body, out) -> {
            serverHits.incrementAndGet();
            writeHttpResponse(out, 200, "OK", "{\"access_token\":\"leaked\"}", false);
          })) {
        OAuthHttpClient client = newClient(mismatchSsl, 1024, host -> new InetAddress[] {loopback});
        HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + server.getPort() + "/token"))
            .addHeader("Authorization", "Basic dXNlcjpwYXNz")
            .withBody("grant_type=refresh_token&refresh_token=secret").build();
        try {
          client.execute(req, AllowedHosts.allowAll(), true);
          Assert.fail("Expected TLS hostname verification failure for SAN " + mismatchedSan);
        } catch (IOException expected) {
          // SSLHandshakeException or IOException("HTTPS hostname wrong")
        }
        Assert.assertEquals("HTTP request must not be sent for SAN " + mismatchedSan, 0, serverHits.get());
      }
    }
  }

  @Test
  public void testNoRetryAfterConnectAndFailureWhenAllAddressesRefuse() throws Exception {
    AtomicInteger primaryHits = new AtomicInteger();
    AtomicInteger secondaryHits = new AtomicInteger();

    try (MiniTlsServer primaryServer = new MiniTlsServer(testSslContext, loopback, 0,
        (sni, path, headers, body, out) -> {
          primaryHits.incrementAndGet();
          out.close();
        })) {
      int port = primaryServer.getPort();
      InetAddress secondary = secondaryLoopbackOrSkip(port, loopback);
      try (MiniTlsServer secondaryServer = new MiniTlsServer(testSslContext, secondary, port,
          (sni, path, headers, body, out) -> {
            secondaryHits.incrementAndGet();
            writeHttpResponse(out, 200, "OK", "{\"access_token\":\"replayed\"}", false);
          })) {
        HttpRequest req = HttpRequest.post(new URL("https://" + TEST_HOST + ":" + port + "/token"))
            .withBody("code=one_time_code&grant_type=authorization_code").build();
        try {
          newClient(loopback, secondary).execute(req, AllowedHosts.allowAll(), true);
          Assert.fail("Expected IOException when server drops connection after receiving request");
        } catch (IOException expected) {
          // expected
        }
        Assert.assertEquals("Request must be sent exactly once", 1, primaryHits.get());
        Assert.assertEquals("Request must not fall back after connect", 0, secondaryHits.get());
      }
    }

    int unusedPort;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      unusedPort = probe.getLocalPort();
    }
    InetAddress secondary = secondaryLoopbackOrSkip(unusedPort, loopback);
    try {
      newClient(loopback, secondary).execute(
          HttpRequest.get(new URL("https://" + TEST_HOST + ":" + unusedPort + "/token")).build(),
          AllowedHosts.allowAll(), true);
      Assert.fail("Expected ConnectException when all addresses refuse");
    } catch (ConnectException expected) {
      // expected
    }
  }

  private static void writeHttpResponse(OutputStream out, int code, String reason,
                                        String body, boolean gzip) throws IOException {
    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
    if (gzip) {
      ByteArrayOutputStream gzOut = new ByteArrayOutputStream();
      try (GZIPOutputStream gz = new GZIPOutputStream(gzOut)) {
        gz.write(payload);
      }
      payload = gzOut.toByteArray();
    }
    String header = "HTTP/1.1 " + code + " " + reason + "\r\n"
        + (gzip ? "Content-Encoding: gzip\r\n" : "")
        + "Content-Length: " + payload.length + "\r\nConnection: close\r\n\r\n";
    out.write(header.getBytes(StandardCharsets.US_ASCII));
    out.write(payload);
    out.flush();
  }

  private static InetAddress secondaryLoopbackOrSkip(int port, InetAddress exclude) throws IOException {
    for (String candidate : new String[] {"127.0.0.2", "::1", "127.0.0.1"}) {
      InetAddress address = InetAddress.getByName(candidate);
      if (address.equals(exclude)) {
        continue;
      }
      try (ServerSocket probe = new ServerSocket(port, 1, address)) {
        return address;
      } catch (IOException ignored) {
        // Try next candidate
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

  private static SSLContext createTestSslContext(String cn, GeneralName san) throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048, new SecureRandom());
    KeyPair keyPair = keyGen.generateKeyPair();

    long now = System.currentTimeMillis();
    X500Name dn = new X500Name("CN=" + cn);
    JcaX509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
        dn, BigInteger.valueOf(now), new Date(now - TimeUnit.HOURS.toMillis(1)),
        new Date(now + TimeUnit.DAYS.toMillis(7)), dn, keyPair.getPublic());
    certBuilder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(san));
    BouncyCastleProvider bc = new BouncyCastleProvider();
    X509Certificate cert = new JcaX509CertificateConverter().setProvider(bc)
        .getCertificate(certBuilder.build(new JcaContentSignerBuilder("SHA256WithRSA").setProvider(bc)
                                              .build(keyPair.getPrivate())));

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
              for (SNIServerName name : ((ExtendedSSLSession) socket.getSession()).getRequestedServerNames()) {
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
