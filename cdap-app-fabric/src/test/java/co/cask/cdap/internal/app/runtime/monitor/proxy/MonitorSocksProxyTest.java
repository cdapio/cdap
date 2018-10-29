/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.monitor.proxy;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.ssh.DefaultSSHSession;
import co.cask.cdap.common.ssh.SSHConfig;
import co.cask.cdap.common.ssh.TestSSHServer;
import co.cask.cdap.runtime.spi.ssh.PortForwarding;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyProducer;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

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
    proxyServer = new MonitorSocksProxy(CConfiguration.create(), host ->
      Optional.ofNullable(sshSession).orElseThrow(() -> new IllegalArgumentException("No SSH session available for "
                                                                                       + host)));
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
  @Ignore
  @Test
  public void testSocksProxy() throws Exception {
    InetSocketAddress httpAddr = httpService.getBindAddress();

    // Make 10 requests. With connection keep-alive, there should only be one SSH tunnel created
    URL url = new URL(String.format("http://%s:%d/ping", httpAddr.getHostName(), httpAddr.getPort()));
    for (int i = 0; i < 10; i++) {
      HttpResponse response = HttpRequests.execute(co.cask.common.http.HttpRequest.get(url).build());
      Assert.assertEquals(200, response.getResponseCode());
    }

    Assert.assertEquals(1, sshSession.portForwardCreated.get());

    // Make one more call with Connection: close. This should close the connection, hence close the tunnel.
    HttpResponse response = HttpRequests.execute(
      co.cask.common.http.HttpRequest.builder(HttpMethod.GET, url)
        .addHeader(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString())
        .build());
    Assert.assertEquals(200, response.getResponseCode());

    Assert.assertEquals(1, sshSession.portForwardCreated.get());
    Assert.assertEquals(1, sshSession.portForwardClosed.get());
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
    HttpRequests.execute(co.cask.common.http.HttpRequest.get(url).build());
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
      HttpRequests.execute(co.cask.common.http.HttpRequest.get(url).build());
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

  /**
   * A {@link HttpHandler} for testing.
   */
  public static final class TestHandler extends AbstractHttpHandler {

    @GET
    @Path("/ping")
    public void ping(HttpRequest request, HttpResponder responder) {
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("/chunk")
    public void chunk(FullHttpRequest request, HttpResponder responder) {
      ByteBuf content = request.content().copy();

      responder.sendContent(HttpResponseStatus.OK, new BodyProducer() {

        int count = 0;

        @Override
        public ByteBuf nextChunk() {
          if (count++ < 10) {
            return content.copy();
          }
          return Unpooled.EMPTY_BUFFER;
        }

        @Override
        public void finished() {
          // no-op
        }

        @Override
        public void handleError(@Nullable Throwable cause) {
          // no-op
        }
      }, new DefaultHttpHeaders());
    }
  }
}
