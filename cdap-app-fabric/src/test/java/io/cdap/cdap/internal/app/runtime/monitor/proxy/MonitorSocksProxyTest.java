/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor.proxy;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.ssh.DefaultSSHSession;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.common.ssh.TestSSHServer;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link MonitorSocksProxy}.
 */
public class MonitorSocksProxyTest {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorSocksProxyTest.class);

  @ClassRule
  public static final TestSSHServer SSH_SERVER = new TestSSHServer();

  private static KeyPair keyPair;
  private NettyHttpService httpService;
  private TestSSHSession sshSession;
  private MonitorSocksProxy proxyServer;
  private ProxySelector defaultProxySelector;

  @BeforeClass
  public static void init() throws Exception {
    keyPair = KeyPair.genKeyPair(new JSch(), KeyPair.RSA, 1024);
    SSH_SERVER.addAuthorizedKey(keyPair, "cdap");
  }

  @Before
  public void beforeTest() throws Exception {
    httpService = NettyHttpService.builder("test")
      .setHttpHandlers(new TestHandler())
      .build();
    httpService.start();

    sshSession = new TestSSHSession(getSSHConfig());
    proxyServer = new MonitorSocksProxy(CConfiguration.create(), (serverAddr, dataConsumer) ->
      sshSession.createLocalPortForward("localhost", serverAddr.getPort(), serverAddr.getPort(), dataConsumer));
    proxyServer.startAndWait();

    Proxy proxy = new Proxy(Proxy.Type.SOCKS, proxyServer.getBindAddress());
    defaultProxySelector = ProxySelector.getDefault();

    // Set the proxy for URLConnection
    ProxySelector.setDefault(new ProxySelector() {
      @Override
      public List<Proxy> select(URI uri) {
        return Collections.singletonList(proxy);
      }

      @Override
      public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        LOG.error("Connect failed {} {}", uri, sa, ioe);
      }
    });
  }

  @After
  public void afterTest() throws Exception {
    ProxySelector.setDefault(defaultProxySelector);
    proxyServer.stopAndWait();
    httpService.stop();
  }

  /**
   * This test the normal operations of the SOCKS proxy.
   */
  @Test
  public void testSocksProxy() throws Exception {
    InetSocketAddress httpAddr = httpService.getBindAddress();

    // Make 10 requests. With connection keep-alive, there should only be one SSH tunnel created
    URL url = new URL(String.format("http://%s:%d/ping", httpAddr.getHostName(), httpAddr.getPort()));
    for (int i = 0; i < 10; i++) {
      HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build(),
                                                   new DefaultHttpRequestConfig(false));
      Assert.assertEquals(200, response.getResponseCode());
    }

    Assert.assertEquals(1, sshSession.portForwardCreated.get());

    // Make one more call with Connection: close. This should close the connection, hence close the tunnel.
    HttpResponse response = HttpRequests.execute(
      io.cdap.common.http.HttpRequest.builder(HttpMethod.GET, url)
        .addHeader(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString())
        .build(),
      new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());

    Assert.assertEquals(1, sshSession.portForwardCreated.get());
    Tasks.waitFor(1, () -> sshSession.portForwardClosed.get(), 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void test() throws Exception {
    // Make 10 requests. With connection keep-alive, there should only be one SSH tunnel created
    URL url = new URL("http://metadata/computeMetadata/v1/instance/service-accounts/default/token");
    HttpResponse response = HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url)
                                                   .addHeader("Metadata-Flavor", "Google").build(),
                                                 new DefaultHttpRequestConfig(false));
    Assert.assertEquals(200, response.getResponseCode());
    System.out.println(response.getResponseBodyAsString());
  }

  @Test
  public void testChunkCall() throws Exception {
    InetSocketAddress httpAddr = httpService.getBindAddress();

    URL url = new URL(String.format("http://%s:%d/chunk", httpAddr.getHostName(), httpAddr.getPort()));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    try (OutputStream os = urlConn.getOutputStream()) {
      os.write("Testing".getBytes(StandardCharsets.UTF_8));
    }

    // Verify the response. The server repeats the request body for 10 times
    Assert.assertEquals(200, urlConn.getResponseCode());
    String content = new String(ByteStreams.toByteArray(urlConn.getInputStream()), StandardCharsets.UTF_8);
    Assert.assertEquals(Strings.repeat("Testing", 10), content);
  }

  /**
   * This test failure in creating SSH tunnel
   */
  @Test(expected = IOException.class)
  public void testFailTunnel() throws Exception {
    InetSocketAddress httpAddr = httpService.getBindAddress();

    // Close the SSH session, hence can't create port forwarding
    sshSession.close();

    // On proxy failure, IOException will be thrown
    URL url = new URL(String.format("http://%s:%d/ping", httpAddr.getHostName(), httpAddr.getPort()));
    HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));
  }

  /**
   * This test failure in looking up a {@link SSHSession} to use
   */
  @Test(expected = IOException.class)
  public void testMissingSSHSession() throws IOException {
    TestSSHSession oldSession = sshSession;
    // Remove the ssh session, hence the lookup will fail
    sshSession = null;
    try {
      // On proxy failure, IOException will be thrown
      InetSocketAddress httpAddr = httpService.getBindAddress();
      URL url = new URL(String.format("http://%s:%d/ping", httpAddr.getHostName(), httpAddr.getPort()));
      HttpRequests.execute(io.cdap.common.http.HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));
    } finally {
      sshSession = oldSession;
    }
  }

  /**
   * Creates a {@link SSHConfig} for connecting to the test ssh server.
   */
  private SSHConfig getSSHConfig() {
    return SSHConfig.builder(SSH_SERVER.getHost())
      .setPort(SSH_SERVER.getPort())
      .setUser("cdap")
      .setPrivateKeySupplier(() -> {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        keyPair.writePrivateKey(bos, null);
        return bos.toByteArray();
      })
      .build();
  }

  /**
   * A {@link SSHSession} for testing. It wraps and instruments method so that it can be inspected later.
   */
  private static final class TestSSHSession extends DefaultSSHSession {

    private final AtomicInteger portForwardCreated = new AtomicInteger();
    private final AtomicInteger portForwardClosed = new AtomicInteger();

    TestSSHSession(SSHConfig config) throws IOException {
      super(config);
    }

    @Override
    public PortForwarding createLocalPortForward(String targetHost, int targetPort, int originatePort,
                                                 PortForwarding.DataConsumer dataConsumer) throws IOException {
      PortForwarding portForwarding = super.createLocalPortForward(targetHost, targetPort, originatePort, dataConsumer);
      portForwardCreated.incrementAndGet();

      return new PortForwarding() {
        @Override
        public int write(ByteBuffer buf) throws IOException {
          return portForwarding.write(buf);
        }

        @Override
        public void flush() throws IOException {
          portForwarding.flush();
        }

        @Override
        public boolean isOpen() {
          return portForwarding.isOpen();
        }

        @Override
        public void close() throws IOException {
          portForwarding.close();
          portForwardClosed.incrementAndGet();
        }
      };
    }
  }

}
