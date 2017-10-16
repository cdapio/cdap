/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.ChannelPipelineModifier;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
  static final int CONNECTION_IDLE_TIMEOUT_SECS = 2;
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterTestBase.class);

  private static final String HOSTNAME = "127.0.0.1";
  private static final String DEFAULT_SERVICE = Constants.Router.GATEWAY_DISCOVERY_NAME;
  private static final String APP_FABRIC_SERVICE = Constants.Service.APP_FABRIC_HTTP;
  private static final int MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
  private static final int CHUNK_SIZE = 1024 * 1024;      // NOTE: MAX_UPLOAD_BYTES % CHUNK_SIZE == 0

  private final DiscoveryService discoveryService = new InMemoryDiscoveryService();
  private final RouterService routerService = createRouterService(HOSTNAME, discoveryService);
  private final ServerService defaultServer1 = new ServerService(HOSTNAME, discoveryService, APP_FABRIC_SERVICE);
  private final ServerService defaultServer2 = new ServerService(HOSTNAME, discoveryService, APP_FABRIC_SERVICE);
  private final List<ServerService> allServers = Lists.newArrayList(defaultServer1, defaultServer2);

  protected abstract RouterService createRouterService(String hostname, DiscoveryService discoveryService);
  protected abstract String getProtocol();
  protected abstract DefaultHttpClient getHTTPClient() throws Exception;
  protected abstract SocketFactory getSocketFactory() throws Exception;

  protected int lookupService(String serviceName) {
    return routerService.lookupService(serviceName);
  }

  private String resolveURI(String serviceName, String path) throws URISyntaxException {
    return getBaseURI(serviceName).resolve(path).toASCIIString();
  }

  private URI getBaseURI(String serviceName) throws URISyntaxException {
    int servicePort = lookupService(serviceName);
    return new URI(String.format("%s://%s:%d", getProtocol(), HOSTNAME, servicePort));
  }

  @Before
  public void startUp() throws Exception {
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
        .setUrl(resolveURI(DEFAULT_SERVICE, String.format("%s/%s-%d", "/v1/echo", "async", i)))
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
    Assert.assertTrue(numElements == (defaultServer1.getNumRequests() + defaultServer2.getNumRequests()));
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
    HttpResponse response = get(resolveURI(DEFAULT_SERVICE, String.format("%s/%s", "/v1/ping", "sync")));
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
      .setUrl(resolveURI(DEFAULT_SERVICE, "/v1/upload"))
      .setContentLength(requestBody.length)
      .setBody(new ByteEntityWriter(requestBody))
      .build();

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Future<Void> future = asyncHttpClient.executeRequest(request, new AsyncCompletionHandler<Void>() {
      @Override
      public Void onCompleted(Response response) throws Exception {
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
      new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/abc/v1/status")),
      new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/def/v1/status"))
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
    URI uri = new URI(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, path));
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

    URL url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v2/ping"));
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
      url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v1/ping/" + i));
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
    url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v2/ping"));
    urlConnection = openURL(url);
    Assert.assertEquals(200, urlConnection.getResponseCode());
    urlConnection.getInputStream().close();
    urlConnection.disconnect();
  }

  @Test
  public void testConnectionNoIdleTimeout() throws Exception {
    // even though the handler will sleep for 500ms over the configured idle timeout before responding, the connection
    // is not closed because the http request is in progress
    long timeoutMillis = TimeUnit.SECONDS.toMillis(CONNECTION_IDLE_TIMEOUT_SECS) + 500;
    URL url = new URL(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME, "/v1/timeout/" + timeoutMillis));
    HttpURLConnection urlConnection = openURL(url);
    Assert.assertEquals(200, urlConnection.getResponseCode());
    urlConnection.disconnect();
  }

  protected HttpURLConnection openURL(URL url) throws Exception {
    return (HttpURLConnection) url.openConnection();
  }

  private void testSync(int numRequests) throws Exception {
    for (int i = 0; i < numRequests; ++i) {
      LOG.trace("Sending sync request " + i);
      HttpResponse response = get(resolveURI(Constants.Router.GATEWAY_DISCOVERY_NAME,
                                             String.format("%s/%s-%d", "/v1/ping", "sync", i)));
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    }
  }

  private void testSyncServiceUnavailable() throws Exception {
    for (int i = 0; i < 25; ++i) {
      LOG.trace("Sending sync unavailable request " + i);
      HttpResponse response = get(resolveURI(DEFAULT_SERVICE, String.format("%s/%s-%d", "/v1/ping", "sync", i)));
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
    public abstract int lookupService(String serviceName);
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

      @POST
      @Path("/v1/upload")
      public void upload(FullHttpRequest request, HttpResponder responder) throws IOException {
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
    }
  }
}
