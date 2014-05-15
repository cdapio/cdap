package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.guice.SecurityModules;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Tests Netty Router.
 */
public class NettyRouterTest {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterTest.class);
  private static final String HOSTNAME = "127.0.0.1";
  private static final DiscoveryService DISCOVERY_SERVICE = new InMemoryDiscoveryService();
  private static final String DEFAULT_SERVICE = Constants.Service.APP_FABRIC_HTTP;
  private static final String WEBAPP_SERVICE = "$HOST";
  private static final int MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
  private static final int CHUNK_SIZE = 1024 * 1024;      // NOTE: MAX_UPLOAD_BYTES % CHUNK_SIZE == 0

  private static final Supplier<String> defaultServiceSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return DEFAULT_SERVICE;
    }
  };

  private static final Supplier<String> webappServiceSupplier = new Supplier<String>() {
    @Override
    public String get() {
      try {
        return Networks.normalizeWebappDiscoveryName(HOSTNAME + ":" + ROUTER.getServiceMap().get(WEBAPP_SERVICE));
      } catch (UnsupportedEncodingException e) {
        LOG.error("Got exception: ", e);
        throw Throwables.propagate(e);
      }
    }
  };

  private static final Supplier<String> defaultWebappServiceSupplier1 = new Supplier<String>() {
    @Override
    public String get() {
      try {
        return Networks.normalizeWebappDiscoveryName("default/abc");
      } catch (UnsupportedEncodingException e) {
        LOG.error("Got exception: ", e);
        throw Throwables.propagate(e);
      }
    }
  };

  private static final Supplier<String> defaultWebappServiceSupplier2 = new Supplier<String>() {
    @Override
    public String get() {
      try {
        return Networks.normalizeWebappDiscoveryName("default/def");
      } catch (UnsupportedEncodingException e) {
        LOG.error("Got exception: ", e);
        throw Throwables.propagate(e);
      }
    }
  };

  public static final RouterResource ROUTER = new RouterResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                 ImmutableSet.of("0:" + DEFAULT_SERVICE,
                                                                                 "0:" + WEBAPP_SERVICE));

  public static final ServerResource DEFAULT_SERVER_1 = new ServerResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                           defaultServiceSupplier);
  public static final ServerResource DEFAULT_SERVER_2 = new ServerResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                           defaultServiceSupplier);
  public static final ServerResource WEBAPP_SERVER = new ServerResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                        webappServiceSupplier);
  public static final ServerResource DEFAULT_WEBAPP_SERVER_1 = new ServerResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                                  defaultWebappServiceSupplier1);
  public static final ServerResource DEFAULT_WEBAPP_SERVER_2 = new ServerResource(HOSTNAME, DISCOVERY_SERVICE,
                                                                                  defaultWebappServiceSupplier2);

  @SuppressWarnings("UnusedDeclaration")
  @ClassRule
  public static TestRule chain = RuleChain.outerRule(ROUTER).around(DEFAULT_SERVER_1)
    .around(DEFAULT_SERVER_2).around(WEBAPP_SERVER).around(DEFAULT_WEBAPP_SERVER_1).around(DEFAULT_WEBAPP_SERVER_2);

  @Before
  public void clearNumRequests() throws Exception {
    DEFAULT_SERVER_1.clearNumRequests();
    DEFAULT_SERVER_2.clearNumRequests();
    WEBAPP_SERVER.clearNumRequests();

    // Wait for both servers of defaultService to be registered
    Iterable<Discoverable> discoverables = ((DiscoveryServiceClient) DISCOVERY_SERVICE).discover(
      defaultServiceSupplier.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 2; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Wait for server of webappService to be registered
    discoverables = ((DiscoveryServiceClient) DISCOVERY_SERVICE).discover(webappServiceSupplier.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Wait for server of defaultWebappServiceSupplier1 to be registered
    discoverables = ((DiscoveryServiceClient) DISCOVERY_SERVICE).discover(defaultWebappServiceSupplier1.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Wait for server of defaultWebappServiceSupplier2 to be registered
    discoverables = ((DiscoveryServiceClient) DISCOVERY_SERVICE).discover(defaultWebappServiceSupplier2.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  @Test
  public void testRouterSync() throws Exception {
    testSync(25);
    // sticky endpoint strategy used so the sum should be 25
    Assert.assertEquals(25, DEFAULT_SERVER_1.getNumRequests() + DEFAULT_SERVER_2.getNumRequests());
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
        .setUrl(String.format("http://%s:%d%s/%s-%d",
                              HOSTNAME, ROUTER.getServiceMap().get(DEFAULT_SERVICE), "/v1/ping", "async", i))
        .build();
      asyncHttpClient.executeRequest(request,
                                     new AsyncCompletionHandler<Void>() {
                                       @Override
                                       public Void onCompleted(Response response) throws Exception {
                                         latch.countDown();
                                         Assert.assertEquals(HttpResponseStatus.OK.getCode(),
                                                             response.getStatusCode());
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
    Assert.assertTrue(numElements == (DEFAULT_SERVER_1.getNumRequests() + DEFAULT_SERVER_2.getNumRequests()));
  }

  @Test
  public void testRouterOneServerDown() throws Exception {
    try {
      // Bring down DEFAULT_SERVER_1
      DEFAULT_SERVER_1.cancelRegistration();

      testSync(25);
    } finally {
      Assert.assertEquals(0, DEFAULT_SERVER_1.getNumRequests());
      Assert.assertTrue(DEFAULT_SERVER_2.getNumRequests() > 0);

      DEFAULT_SERVER_1.registerServer();
    }
  }

  @Test
  public void testRouterAllServersDown() throws Exception {
    try {
      // Bring down all servers
      DEFAULT_SERVER_1.cancelRegistration();
      DEFAULT_SERVER_2.cancelRegistration();

      testSyncServiceUnavailable();
    } finally {
      Assert.assertEquals(0, DEFAULT_SERVER_1.getNumRequests());
      Assert.assertEquals(0, DEFAULT_SERVER_2.getNumRequests());

      DEFAULT_SERVER_1.registerServer();
      DEFAULT_SERVER_2.registerServer();
    }
  }

  @Test
  public void testHostForward() throws Exception {
    // Test defaultService
    HttpResponse response = get(String.format("http://%s:%d%s/%s",
                                              HOSTNAME, ROUTER.getServiceMap().get(DEFAULT_SERVICE),
                                              "/v1/ping", "sync"));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(defaultServiceSupplier.get(), EntityUtils.toString(response.getEntity()));

    // Test webappService
    response = get(String.format("http://%s:%d%s/%s",
                                 HOSTNAME, ROUTER.getServiceMap().get(WEBAPP_SERVICE), "/v1/ping", "sync"));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(webappServiceSupplier.get(), EntityUtils.toString(response.getEntity()));

    // Test default
    response = get(String.format("http://%s:%d%s/%s",
                                 HOSTNAME, ROUTER.getServiceMap().get(WEBAPP_SERVICE), "/abc/v1/ping", "sync"),
                   new Header[]{new BasicHeader(HttpHeaders.Names.HOST, "www.abc.com")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(defaultWebappServiceSupplier1.get(), EntityUtils.toString(response.getEntity()));

    // Test default, port 80
    response = get(String.format("http://%s:%d%s/%s",
                                 HOSTNAME, ROUTER.getServiceMap().get(WEBAPP_SERVICE), "/abc/v1/ping", "sync"),
                   new Header[]{new BasicHeader(HttpHeaders.Names.HOST, "www.def.com" + ":80")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(defaultWebappServiceSupplier1.get(), EntityUtils.toString(response.getEntity()));

    // Test default, port random port
    response = get(String.format("http://%s:%d%s/%s",
                                 HOSTNAME, ROUTER.getServiceMap().get(WEBAPP_SERVICE), "/def/v1/ping", "sync"),
                   new Header[]{new BasicHeader(HttpHeaders.Names.HOST, "www.ghi.net" + ":" + "5678")});
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    Assert.assertEquals(defaultWebappServiceSupplier2.get(), EntityUtils.toString(response.getEntity()));
  }

  @Test
  public void testUpload() throws Exception {
    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    byte [] requestBody = generatePostData();
    final Request request = new RequestBuilder("POST")
      .setUrl(String.format("http://%s:%d%s", HOSTNAME, ROUTER.getServiceMap().get(DEFAULT_SERVICE), "/v1/upload"))
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

  private void testSync(int numRequests) throws Exception {
    for (int i = 0; i < numRequests; ++i) {
      LOG.trace("Sending request " + i);
      HttpResponse response = get(String.format("http://%s:%d%s/%s-%d",
                                                HOSTNAME, ROUTER.getServiceMap().get(DEFAULT_SERVICE),
                                                "/v1/ping", "sync", i));
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    }
  }

  private void testSyncServiceUnavailable() throws Exception {
    for (int i = 0; i < 25; ++i) {
      LOG.trace("Sending request " + i);
      HttpResponse response = get(String.format("http://%s:%d%s/%s-%d",
                                                HOSTNAME, ROUTER.getServiceMap().get(DEFAULT_SERVICE),
                                                "/v1/ping", "sync", i));
      Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatusLine().getStatusCode());
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
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(url);
    if (headers != null) {
      get.setHeaders(headers);
    }
    return client.execute(get);
  }

  private static class RouterResource extends ExternalResource {
    private final String hostname;
    private final DiscoveryService discoveryService;
    private final Set<String> forwards;
    private final Map<String, Integer> serviceMap = Maps.newHashMap();

    private NettyRouter router;

    private RouterResource(String hostname, DiscoveryService discoveryService, Set<String> forwards) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
      this.forwards = forwards;
    }

    @Override
    protected void before() throws Throwable {
      CConfiguration cConf = CConfiguration.create();
      Injector injector = Guice.createInjector(new IOModule(), new SecurityModules().getInMemoryModules(),
                                               new DiscoveryRuntimeModule().getInMemoryModules());
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
      cConf.set(Constants.Router.ADDRESS, hostname);
      cConf.setStrings(Constants.Router.FORWARD, forwards.toArray(new String[forwards.size()]));
      router =
        new NettyRouter(cConf, InetAddresses.forString(hostname),
                        new RouterServiceLookup((DiscoveryServiceClient) discoveryService,
                                                new RouterPathLookup(new NoAuthenticator())),
                        new SuccessTokenValidator(), accessTokenTransformer, discoveryServiceClient);
      router.startAndWait();

      for (Map.Entry<Integer, String> entry : router.getServiceLookup().getServiceMap().entrySet()) {
        serviceMap.put(entry.getValue(), entry.getKey());
      }
    }

    @Override
    protected void after() {
      router.stopAndWait();
    }

    public Map<String, Integer> getServiceMap() {
      return serviceMap;
    }
  }

  /**
   * A generic server for testing router.
   */
  public static class ServerResource extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(ServerResource.class);

    private final String hostname;
    private final DiscoveryService discoveryService;
    private final Supplier<String> serviceNameSupplier;
    private final AtomicInteger numRequests = new AtomicInteger(0);

    private NettyHttpService httpService;
    private Cancellable cancelDiscovery;

    private ServerResource(String hostname, DiscoveryService discoveryService, Supplier<String> serviceNameSupplier) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
      this.serviceNameSupplier = serviceNameSupplier;
    }

    @Override
    protected void before() throws Throwable {
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableSet.of(new ServerHandler()));
      builder.setHost(hostname);
      builder.setPort(0);
      httpService = builder.build();
      httpService.startAndWait();

      registerServer();

      log.info("Started test server on {}", httpService.getBindAddress());
    }

    @Override
    protected void after() {
      httpService.stopAndWait();
    }

    public int getNumRequests() {
      return numRequests.get();
    }

    public void clearNumRequests() {
      numRequests.set(0);
    }

    public void registerServer() {
      // Register services of test server
      log.info("Registering service {}", serviceNameSupplier.get());
      cancelDiscovery = discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return serviceNameSupplier.get();
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return httpService.getBindAddress();
        }
      });
    }

    public void cancelRegistration() {
      cancelDiscovery.cancel();
    }

    /**
     * Simple handler for server.
     */
    public class ServerHandler extends AbstractHttpHandler {
      private final Logger log = LoggerFactory.getLogger(ServerHandler.class);
      @GET
      @Path("/v1/ping/{text}")
      public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get());
      }

      @GET
      @Path("/abc/v1/ping/{text}")
      public void abcPing(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get());
      }

      @GET
      @Path("/def/v1/ping/{text}")
      public void defPing(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        log.trace("Got text {}", text);

        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get());
      }

      @GET
      @Path("/v2/ping")
      public void gateway(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder) {
        numRequests.incrementAndGet();

        responder.sendString(HttpResponseStatus.OK, serviceNameSupplier.get());
      }

      @POST
      @Path("/v1/upload")
      public void upload(HttpRequest request, final HttpResponder responder) throws InterruptedException {
        ChannelBuffer content = request.getContent();

        int readableBytes;
        responder.sendChunkStart(HttpResponseStatus.OK, ImmutableMultimap.<String, String>of());
        while ((readableBytes = content.readableBytes()) > 0) {
          int read = Math.min(readableBytes, CHUNK_SIZE);
          responder.sendChunk(content.readSlice(read));
          //TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(1));
        }
        responder.sendChunkEnd();
      }
    }
  }
}
