/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.http.AbstractBodyConsumer;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.SocketFactory;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Tests Netty Router.
 */
public abstract class NettyRouterTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  static final int CONNECTION_IDLE_TIMEOUT_SECS = 2;
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterTestBase.class);

  private static final String HOSTNAME = InetAddress.getLoopbackAddress().getHostAddress();
  private static final String APP_FABRIC_SERVICE = Constants.Service.APP_FABRIC_HTTP;
  private static final int MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
  private static final int CHUNK_SIZE = 1024 * 1024;      // NOTE: MAX_UPLOAD_BYTES % CHUNK_SIZE == 0

  private final DiscoveryService discoveryService = new InMemoryDiscoveryService();
  private final ServerService defaultServer1 = new ServerService(HOSTNAME, discoveryService, APP_FABRIC_SERVICE);
  private final ServerService defaultServer2 = new ServerService(HOSTNAME, discoveryService, APP_FABRIC_SERVICE);
  private final List<ServerService> allServers = Lists.newArrayList(defaultServer1, defaultServer2);
  private RouterService routerService;

  protected abstract RouterService createRouterService(String hostname, DiscoveryService discoveryService);
  protected abstract String getProtocol();
  protected abstract DefaultHttpClient getHTTPClient() throws Exception;
  protected abstract SocketFactory getSocketFactory() throws Exception;

  private String resolveURI(String path) throws URISyntaxException {
    InetSocketAddress address = routerService.getRouterAddress();
    return new URI(String.format("%s://%s:%d", getProtocol(),
                                 address.getHostName(), address.getPort())).resolve(path).toASCIIString();
  }

  @Before
  public void startUp() throws Exception {
    routerService = createRouterService(HOSTNAME, discoveryService);
    List<ListenableFuture<Service.State>> futures = new ArrayList<>();
    futures.add(routerService.start());
    for (ServerService server : allServers) {
      futures.add(server.start());
    }
    Futures.allAsList(futures).get();

    // Wait for both servers of defaultService to be registered
    ServiceDiscovered discover = ((DiscoveryServiceClient) discoveryService).discover(APP_FABRIC_SERVICE);
    final CountDownLatch latch = new CountDownLatch(1);
    Cancellable cancellable = discover.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        if (Iterables.size(serviceDiscovered) == allServers.size()) {
          latch.countDown();
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    cancellable.cancel();
  }

  @After
  public void tearDown() throws Exception {
    List<ListenableFuture<Service.State>> futures = new ArrayList<>();
    for (ServerService server : allServers) {
      futures.add(server.stop());
    }
    futures.add(routerService.stop());
    Futures.successfulAsList(futures).get();
  }

  @Test
  public void testRouterSync() throws Exception {
    testSync(25);
    // sticky endpoint strategy used so the sum should be 25
    Assert.assertEquals(25, defaultServer1.getNumRequests() + defaultServer2.getNumRequests());
  }

  @Test
  public void testRouterAsync() throws Exception {
    int numElements = 123;
    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    final CountDownLatch latch = new CountDownLatch(numElements);
    final AtomicInteger numSuccessfulRequests = new AtomicInteger(0);
    for (int i = 0; i < numElements; ++i) {
      final int elem = i;
      final Request request = new RequestBuilder("GET")
        .setUrl(resolveURI(String.format("%s/%s-%d", "/v1/echo", "async", i)))
        .build();
      asyncHttpClient.executeRequest(request, new AsyncCompletionHandler<Void>() {
        @Override
        public Void onCompleted(Response response) throws Exception {
          latch.countDown();
          Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusCode());
          String responseBody = response.getResponseBody();
          LOG.trace("Got response {}", responseBody);
          Assert.assertEquals("async-" + elem, responseBody);
          numSuccessfulRequests.incrementAndGet();
          return null;
        }

        @Override
        public void onThrowable(Throwable t) {
          LOG.error("Got exception while posting {}", elem, t);
          latch.countDown();

        }
      });

      // Sleep so as not to overrun the server.
      TimeUnit.MILLISECONDS.sleep(1);
    }
    latch.await();
    asyncHttpClient.close();

    Assert.assertEquals(numElements, numSuccessfulRequests.get());
    // we use sticky endpoint strategy so the sum of requests from the two gateways should be NUM_ELEMENTS
    Assert.assertEquals(numElements, (defaultServer1.getNumRequests() + defaultServer2.getNumRequests()));
  }

  @Test
  public void testRouterOneServerDown() throws Exception {
    // Bring down defaultServer1
    defaultServer1.cancelRegistration();

    testSync(25);
    Assert.assertEquals(0, defaultServer1.getNumRequests());
    Assert.assertTrue(defaultServer2.getNumRequests() > 0);

    defaultServer1.registerServer();
  }

  @Test
  public void testRouterAllServersDown() throws Exception {
    // Bring down all servers
    defaultServer1.cancelRegistration();
    defaultServer2.cancelRegistration();

    testSyncServiceUnavailable();
    Assert.assertEquals(0, defaultServer1.getNumRequests());
    Assert.assertEquals(0, defaultServer2.getNumRequests());

    defaultServer1.registerServer();
    defaultServer2.registerServer();
  }

  @Test
  public void testHostForward() throws Exception {
    // Test defaultService
    HttpResponse response = get(resolveURI(String.format("%s/%s", "/v1/ping", "sync")));
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(APP_FABRIC_SERVICE, EntityUtils.toString(response.getEntity()));
  }

  @Test
  public void testUpload() throws Exception {
    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    byte [] requestBody = generatePostData();
    final Request request = new RequestBuilder("POST")
      .setUrl(resolveURI("/v1/upload"))
      .setContentLength(requestBody.length)
      .setBody(new ByteEntityWriter(requestBody))
      .build();

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Future<Void> future = asyncHttpClient.executeRequest(request, new AsyncCompletionHandler<Void>() {
      @Override
      public Void onCompleted(Response response) {
        return null;
      }

      @Override
      public STATE onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
        //TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(10));
        content.writeTo(byteArrayOutputStream);
        return super.onBodyPartReceived(content);
      }
    });

    future.get();
    Assert.assertArrayEquals(requestBody, byteArrayOutputStream.toByteArray());
  }

  @Test
  public void testConnectionClose() throws Exception {
    URL[] urls = new URL[] {
      new URL(resolveURI("/abc/v1/status")),
      new URL(resolveURI("/def/v1/status"))
    };

    // Make bunch of requests to one service to 2 difference urls, with the first one keep-alive, second one not.
    // This make router creates two backend service connections on the same inbound connection
    // This is to verify on the close of the second one, it won't close the the inbound if there is an
    // in-flight request happening already (if reached another round of the following for-loop).
    int times = 1000;
    boolean keepAlive = true;
    for (int i = 0; i < times; i++) {
      HttpURLConnection urlConn = openURL(urls[i % urls.length]);
      try {
        urlConn.setRequestProperty(HttpHeaderNames.CONNECTION.toString(),
                                   (keepAlive ? HttpHeaderValues.KEEP_ALIVE : HttpHeaderValues.CLOSE).toString());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, urlConn.getResponseCode());
      } finally {
        urlConn.getInputStream().close();
        keepAlive = !keepAlive;
        urlConn.disconnect();
      }
    }

    Assert.assertEquals(times, defaultServer1.getNumRequests() + defaultServer2.getNumRequests());
  }

  @Test
  public void testConnectionIdleTimeout() throws Exception {
    // Only use server1
    defaultServer2.cancelRegistration();

    String path = "/v2/ping";
    URI uri = new URI(resolveURI(path));
    Socket socket = getSocketFactory().createSocket(uri.getHost(), uri.getPort());
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    InputStream inputStream = socket.getInputStream();

    // make a request
    String firstLine = makeRequest(uri, out, inputStream);
    Assert.assertEquals("HTTP/1.1 200 OK", firstLine);

    // sleep for 500 ms below the configured idle timeout; the connection on server side should not get closed by then
    // Hence it should be reusing the same server side connection
    TimeUnit.MILLISECONDS.sleep(TimeUnit.SECONDS.toMillis(CONNECTION_IDLE_TIMEOUT_SECS) - 500);
    firstLine = makeRequest(uri, out, inputStream);
    Assert.assertEquals("HTTP/1.1 200 OK", firstLine);

    // sleep for 500 ms over the configured idle timeout; the connection on server side should get closed by then
    // Hence it should be create a new server side connection
    TimeUnit.MILLISECONDS.sleep(TimeUnit.SECONDS.toMillis(CONNECTION_IDLE_TIMEOUT_SECS) + 500);
    // Due to timeout the client connection will be closed, and hence this request should not go to the server
    firstLine = makeRequest(uri, out, inputStream);
    Assert.assertEquals("HTTP/1.1 200 OK", firstLine);

    // assert that the connection is closed on the server side
    Assert.assertEquals(3, defaultServer1.getNumRequests());
    Assert.assertEquals(2, defaultServer1.getNumConnectionsOpened());
    Assert.assertEquals(1, defaultServer1.getNumConnectionsClosed());
  }

  private String makeRequest(URI uri, PrintWriter out, InputStream inputStream) throws IOException {
    //Send request
    out.print("GET " + uri.getPath() + " HTTP/1.1\r\n" +
                "Host: " + uri.getHost() + "\r\n" +
                "Connection: keep-alive\r\n\r\n");
    out.flush();

    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    // Read the first line and find
    String line = reader.readLine();
    String firstLine = line;
    int contentLength = 0;
    while (!line.isEmpty()) {
      if (line.toLowerCase().startsWith(HttpHeaderNames.CONTENT_LENGTH.toString())) {
        contentLength = Integer.parseInt(line.split(":", 2)[1].trim());
      }
      line = reader.readLine();
    }

    // Read and throw away the body
    for (int i = 0; i < contentLength; i++) {
      reader.read();
    }

    return firstLine;
  }

  @Test
  public void testConnectionIdleTimeoutWithMultipleServers() throws Exception {
    defaultServer2.cancelRegistration();

    URL url = new URL(resolveURI("/v2/ping"));
    HttpURLConnection urlConnection = openURL(url);
    Assert.assertEquals(200, urlConnection.getResponseCode());
    urlConnection.getInputStream().close();
    urlConnection.disconnect();

    // requests past this point will go to defaultServer2
    defaultServer1.cancelRegistration();
    defaultServer2.registerServer();

    for (int i = 0; i < 4; i++) {
      // this is an assumption that CONNECTION_IDLE_TIMEOUT_SECS is more than 1 second
      TimeUnit.SECONDS.sleep(1);
      url = new URL(resolveURI("/v1/ping/" + i));
      urlConnection = openURL(url);
      Assert.assertEquals(200, urlConnection.getResponseCode());
      urlConnection.getInputStream().close();
      urlConnection.disconnect();
    }

    // for the past 4 seconds, we've been making requests to defaultServer2; therefore, defaultServer1 will have closed
    // its single connection
    Assert.assertEquals(1, defaultServer1.getNumConnectionsOpened());
    Assert.assertEquals(1, defaultServer1.getNumConnectionsClosed());

    // however, the connection to defaultServer2 is not timed out, because we've been making requests to it
    Assert.assertEquals(1, defaultServer2.getNumConnectionsOpened());
    Assert.assertEquals(0, defaultServer2.getNumConnectionsClosed());

    defaultServer2.registerServer();
    defaultServer1.cancelRegistration();
    url = new URL(resolveURI("/v2/ping"));
    urlConnection = openURL(url);
    Assert.assertEquals(200, urlConnection.getResponseCode());
    urlConnection.getInputStream().close();
    urlConnection.disconnect();
  }

  @Test (timeout = 5000L)
  public void testExpectContinue() throws Exception {
    URL url = new URL(resolveURI("/v2/upload"));
    HttpURLConnection urlConn = openURL(url);
    urlConn.setRequestMethod("POST");
    urlConn.setRequestProperty(HttpHeaderNames.EXPECT.toString(), HttpHeaderValues.CONTINUE.toString());
    urlConn.setDoOutput(true);

    // Forces sending small chunks to have the netty server receives multiple chunks
    urlConn.setChunkedStreamingMode(10);
    String msg = Strings.repeat("Message", 100);
    urlConn.getOutputStream().write(msg.getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(200, urlConn.getResponseCode());
    String result = new String(ByteStreams.toByteArray(urlConn.getInputStream()), StandardCharsets.UTF_8);
    Assert.assertEquals(msg, result);
  }

  @Test (timeout = 5000L)
  public void testNotFound() throws Exception {
    URL url = new URL(resolveURI("/v1/not/exists"));
    HttpURLConnection urlConn = openURL(url);
    Assert.assertEquals(404, urlConn.getResponseCode());

    // This shouldn't block as the router connection should get closed
    ByteStreams.toByteArray(urlConn.getErrorStream());
  }

  @Test
  public void testConnectionNoIdleTimeout() throws Exception {
    // even though the handler will sleep for 500ms over the configured idle timeout before responding, the connection
    // is not closed because the http request is in progress
    long timeoutMillis = TimeUnit.SECONDS.toMillis(CONNECTION_IDLE_TIMEOUT_SECS) + 500;
    URL url = new URL(resolveURI("/v1/timeout/" + timeoutMillis));
    HttpURLConnection urlConnection = openURL(url);
    Assert.assertEquals(200, urlConnection.getResponseCode());
    urlConnection.disconnect();
  }

  @Test
  public void testConnectionClose2() throws Exception {
    // turn off retry on POST request
    String oldValue = System.getProperty("sun.net.http.retryPost");
    System.setProperty("sun.net.http.retryPost", "false");
    try {
      URL url = new URL(resolveURI("/v1/sleep"));

      // Disable the server 2 from discovery
      defaultServer2.cancelRegistration();

      HttpURLConnection urlConn = openURL(url);
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("POST");

      // Sleep for 50 ms
      urlConn.getOutputStream().write("50".getBytes(StandardCharsets.UTF_8));
      Assert.assertEquals(200, urlConn.getResponseCode());
      urlConn.getInputStream().close();
      urlConn.disconnect();

      // Now disable server1 and enable server2 from discovery
      defaultServer1.cancelRegistration();
      defaultServer2.registerServer();

      // Make sure the discovery change is in effect
      Assert.assertNotNull(new RandomEndpointStrategy(
        () -> ((DiscoveryServiceClient) discoveryService).discover(APP_FABRIC_SERVICE)).pick(5, TimeUnit.SECONDS));

      // Make a call to sleep for couple seconds
      urlConn = openURL(url);
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("POST");
      urlConn.getOutputStream().write("3000".getBytes(StandardCharsets.UTF_8));

      // Wait for the result asynchronously, while at the same time shutdown server1.
      // Shutting down server1 shouldn't affect the connection.
      CompletableFuture<Integer> result = new CompletableFuture<>();
      HttpURLConnection finalUrlConn = urlConn;
      Thread t = new Thread(() -> {
        try {
          result.complete(finalUrlConn.getResponseCode());
        } catch (Exception e) {
          result.completeExceptionally(e);
        } finally {
          try {
            finalUrlConn.getInputStream().close();
            finalUrlConn.disconnect();
          } catch (IOException e) {
            LOG.error("Exception when closing url connection", e);
          }
        }
      });
      t.start();

      defaultServer1.stopAndWait();
      Assert.assertEquals(200, result.get().intValue());
      Assert.assertEquals(1, defaultServer1.getNumRequests());
      Assert.assertEquals(1, defaultServer2.getNumRequests());
    } finally {
      if (oldValue == null) {
        System.clearProperty("sun.net.http.retryPost");
      } else {
        System.setProperty("sun.net.http.retryPost", oldValue);
      }
    }
  }

  @Test
  public void testConfigReloading() throws Exception {
    long reloadIntervalSeconds = 10;

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TokenValidator successValidator = new SuccessTokenValidator();

    // Configure router with some config-reloading time set
    CConfiguration cConfSpy1 = Mockito.spy(CConfiguration.create());
    cConfSpy1.setLong(Constants.Router.CCONF_RELOAD_INTERVAL_SECONDS, reloadIntervalSeconds);
    cConfSpy1.setInt(Constants.Router.ROUTER_PORT, 0);
    NettyRouter router1 = new NettyRouter(cConfSpy1, SConfiguration.create(), InetAddress.getLoopbackAddress(),
                                          new RouterServiceLookup(cConfSpy1, discoveryService, new RouterPathLookup()),
                                          successValidator,
                                          new MockAccessTokenIdentityExtractor(successValidator), discoveryService,
                                          new NoOpMetricsCollectionService());
    router1.startAndWait();

    // Configure router with config-reloading time set to 0
    CConfiguration cConfSpy2 = Mockito.spy(CConfiguration.create());
    cConfSpy2.setLong(Constants.Router.CCONF_RELOAD_INTERVAL_SECONDS, 0);
    cConfSpy2.setInt(Constants.Router.ROUTER_PORT, 0);
    NettyRouter router2 = new NettyRouter(cConfSpy2, SConfiguration.create(), InetAddress.getLoopbackAddress(),
                                          new RouterServiceLookup(cConfSpy2, discoveryService, new RouterPathLookup()),
                                          successValidator,
                                          new MockAccessTokenIdentityExtractor(successValidator), discoveryService,
                                          new NoOpMetricsCollectionService());
    router2.startAndWait();

    // Wait sometime for cConf to reload
    Thread.sleep(TimeUnit.MILLISECONDS.convert(reloadIntervalSeconds + 2, TimeUnit.SECONDS));

    Mockito.verify(cConfSpy1, Mockito.times(1)).reloadConfiguration();
    Mockito.verify(cConfSpy2, Mockito.never()).reloadConfiguration();
    router1.stopAndWait();
    router2.stopAndWait();
  }

  protected HttpURLConnection openURL(URL url) throws Exception {
    return (HttpURLConnection) url.openConnection();
  }

  private void testSync(int numRequests) throws Exception {
    for (int i = 0; i < numRequests; ++i) {
      LOG.trace("Sending sync request " + i);
      HttpResponse response = get(resolveURI(String.format("%s/%s-%d", "/v1/ping", "sync", i)));
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    }
  }

  private void testSyncServiceUnavailable() throws Exception {
    for (int i = 0; i < 25; ++i) {
      LOG.trace("Sending sync unavailable request " + i);
      HttpResponse response = get(resolveURI(String.format("%s/%s-%d", "/v1/ping", "sync", i)));
      Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), response.getStatusLine().getStatusCode());
    }
  }

  private byte [] generatePostData() {
    byte [] bytes = new byte [MAX_UPLOAD_BYTES];

    for (int i = 0; i < MAX_UPLOAD_BYTES; ++i) {
      bytes[i] = (byte) i;
    }

    return bytes;
  }

  private static class ByteEntityWriter implements Request.EntityWriter {
    private final byte [] bytes;

    private ByteEntityWriter(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public void writeEntity(OutputStream out) throws IOException {
      for (int i = 0; i < MAX_UPLOAD_BYTES; i += CHUNK_SIZE) {
        out.write(bytes, i, CHUNK_SIZE);
      }
    }
  }

  private HttpResponse get(String url) throws Exception {
    return get(url, null);
  }

  private HttpResponse get(String url, Header[] headers) throws Exception {
    DefaultHttpClient client = getHTTPClient();
    HttpGet get = new HttpGet(url);
    if (headers != null) {
      get.setHeaders(headers);
    }
    return client.execute(get);
  }

  /**
   * A server for the router.
   */
  public abstract static class RouterService extends AbstractIdleService {
    public abstract InetSocketAddress getRouterAddress();
  }

  /**
   * A generic server for testing router.
   */
  public static class ServerService extends AbstractIdleService {
    private static final Logger log = LoggerFactory.getLogger(ServerService.class);

    private final String hostname;
    private final DiscoveryService discoveryService;
    private final String serviceName;
    private final AtomicInteger numRequests = new AtomicInteger(0);
    private final AtomicInteger numConnectionsOpened = new AtomicInteger(0);
    private final AtomicInteger numConnectionsClosed = new AtomicInteger(0);

    private NettyHttpService httpService;
    private Cancellable cancelDiscovery;

    private ServerService(String hostname, DiscoveryService discoveryService, String serviceName) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
      this.serviceName = serviceName;
    }

    @Override
    protected void startUp() throws Exception {
      NettyHttpService.Builder builder = NettyHttpService.builder(ServerService.class.getName());
      builder.setHttpHandlers(new ServerHandler());
      builder.setHost(hostname);
      builder.setPort(0);
      builder.setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addLast("connection-counter", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
              numConnectionsOpened.incrementAndGet();
              super.channelActive(ctx);
            }

            @Override
            public void channelUnregistered(io.netty.channel.ChannelHandlerContext ctx) throws Exception {
              numConnectionsClosed.incrementAndGet();
              super.channelInactive(ctx);
            }
          });
        }
      });
      httpService = builder.build();
      httpService.start();

      registerServer();

      log.info("Started test server on {}", httpService.getBindAddress());
    }

    @Override
    protected void shutDown() throws Exception {
      cancelDiscovery.cancel();
      httpService.stop();
    }

    public int getNumRequests() {
      return numRequests.get();
    }

    public int getNumConnectionsOpened() {
      return numConnectionsOpened.get();
    }

    public int getNumConnectionsClosed() {
      return numConnectionsClosed.get();
    }


    public void registerServer() {
      // Register services of test server
      log.info("Registering service {}", serviceName);
      cancelDiscovery = discoveryService.register(
        ResolvingDiscoverable.of(new Discoverable(serviceName, httpService.getBindAddress())));
    }

    public void cancelRegistration() {
      log.info("Cancelling discovery registration of service {}", serviceName);
      cancelDiscovery.cancel();
    }

    /**
     * Simple handler for server.
     */
    public class ServerHandler extends AbstractHttpHandler {
      private final Logger log = LoggerFactory.getLogger(ServerHandler.class);
      @GET
      @Path("/v1/echo/{text}")
      public void echo(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, text);
      }

      @GET
      @Path("/v1/ping/{text}")
      public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceName);
      }

      @GET
      @Path("/abc/v1/ping/{text}")
      public void abcPing(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceName);
      }

      @GET
      @Path("/def/v1/ping/{text}")
      public void defPing(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceName);
      }

      @GET
      @Path("/v2/ping")
      public void gateway(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder) {
        numRequests.incrementAndGet();

        responder.sendString(HttpResponseStatus.OK, serviceName);
      }

      @GET
      @Path("/abc/v1/status")
      public void abcStatus(HttpRequest request, HttpResponder responder) {
        numRequests.incrementAndGet();
        responder.sendStatus(HttpResponseStatus.OK);
      }

      @GET
      @Path("/def/v1/status")
      public void defStatus(HttpRequest request, HttpResponder responder) {
        numRequests.incrementAndGet();
        responder.sendStatus(HttpResponseStatus.OK);
      }

      @GET
      @Path("/v1/timeout/{timeout-millis}")
      public void timeout(HttpRequest request, HttpResponder responder,
                          @PathParam("timeout-millis") int timeoutMillis) throws InterruptedException {
        numRequests.incrementAndGet();
        TimeUnit.MILLISECONDS.sleep(timeoutMillis);
        responder.sendStatus(HttpResponseStatus.OK);
      }

      @GET
      @Path("/v1/exception/{message}")
      public void exception(HttpRequest request, HttpResponder responder,
                            @PathParam("msg") String msg) throws Exception {
        throw new Exception(msg);
      }

      @POST
      @Path("/v1/upload")
      public void upload(FullHttpRequest request, HttpResponder responder) throws IOException {
        numRequests.incrementAndGet();
        ByteBuf content = request.content();

        int readableBytes;
        ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK);
        while ((readableBytes = content.readableBytes()) > 0) {
          int read = Math.min(readableBytes, CHUNK_SIZE);
          chunkResponder.sendChunk(content.readRetainedSlice(read));
          //TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(1));
        }
        chunkResponder.close();
      }

      @POST
      @Path("/v2/upload")
      public BodyConsumer upload2(HttpRequest request, HttpResponder responder) throws Exception {
        numRequests.incrementAndGet();
        // Functionally the same as the upload() above, but use BodyConsumer instead.
        // This is for testing the handling of Expect: 100-continue header
        return new AbstractBodyConsumer(TEMP_FOLDER.newFile()) {
          @Override
          protected void onFinish(HttpResponder responder, File file) throws Exception {
            responder.sendFile(file);
          }
        };
      }

      @POST
      @Path("/v1/sleep")
      public void sleep(FullHttpRequest request, HttpResponder responder) throws Exception {
        numRequests.incrementAndGet();
        long sleepMillis = Long.parseLong(request.content().toString(StandardCharsets.UTF_8));
        TimeUnit.MILLISECONDS.sleep(sleepMillis);

        responder.sendStatus(HttpResponseStatus.OK);
      }
    }
  }
}
