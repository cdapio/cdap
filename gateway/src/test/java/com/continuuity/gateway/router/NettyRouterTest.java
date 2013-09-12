package com.continuuity.gateway.router;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.continuuity.weave.discovery.InMemoryDiscoveryService;
import com.google.common.collect.ImmutableSet;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests Netty Router.
 */
public class NettyRouterTest {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterTest.class);
  private static final String hostname = "127.0.0.1";
  private static final DiscoveryService discoveryService = new InMemoryDiscoveryService();
  private static final String serviceName = "test.service";

  @ClassRule
  public static ServerResource server1 = new ServerResource(hostname, discoveryService, serviceName);
  @ClassRule
  public static ServerResource server2 = new ServerResource(hostname, discoveryService, serviceName);

  @ClassRule
  public static RouterResource router = new RouterResource(hostname, discoveryService, serviceName);

  @Before
  public void clearNumRequests() {
    server1.clearNumRequests();
    server2.clearNumRequests();
  }

  @After
  public void assertServerReqs() {
    Assert.assertTrue(server1.getNumRequests() > 0);
    Assert.assertTrue(server2.getNumRequests() > 0);
  }

  @Test
  public void testRouterSync() throws Exception {
    for (int i = 0; i < 100; ++i) {
      LOG.info("Sending request " + i);
      DefaultHttpClient client = new DefaultHttpClient();
      HttpGet get = new HttpGet(String.format("http://%s:%d%s/%s-%d",
                                              hostname, router.getPort(), "/v1/ping", "sync", i));
      HttpResponse response = client.execute(get);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testRouterAsync() throws Exception {
    int NUM_ELEMENTS = 123;
    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    final CountDownLatch latch = new CountDownLatch(NUM_ELEMENTS);
    final AtomicInteger numSuccessfulRequests = new AtomicInteger(0);
    for (int i = 0; i < NUM_ELEMENTS; ++i) {
      final int elem = i;
      final Request request = new RequestBuilder("GET")
        .setUrl(String.format("http://%s:%d%s/%s-%d",
                              hostname, router.getPort(), "/v1/ping", "async", i))
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

    Assert.assertEquals(NUM_ELEMENTS, numSuccessfulRequests.get());
  }

  private static class RouterResource extends ExternalResource {
    private final String hostname;
    private final DiscoveryService discoveryService;
    private final String serviceName;

    private NettyRouter router;

    private RouterResource(String hostname, DiscoveryService discoveryService, String serviceName) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
      this.serviceName = serviceName;
    }

    @Override
    protected void before() throws Throwable {
      EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
        ((DiscoveryServiceClient) discoveryService).discover(serviceName));

      router = new NettyRouter(1, 5, 100, 1, 5, 100, new InetSocketAddress(hostname, 0), endpointStrategy);
      router.startAndWait();
    }

    @Override
    protected void after() {
      router.stopAndWait();
    }

    public int getPort() {
      return router.getPort();
    }
  }

  /**
   * A generic server for testing router.
   */
  public static class ServerResource extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(ServerResource.class);

    private final String hostname;
    private final DiscoveryService discoveryService;
    private final String serviceName;
    private final AtomicInteger numRequests = new AtomicInteger(0);

    private NettyHttpService httpService;

    private ServerResource(String hostname, DiscoveryService discoveryService, String serviceName) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
      this.serviceName = serviceName;
    }

    @Override
    protected void before() throws Throwable {
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableSet.of(new ServerHandler()));
      builder.setHost(hostname);
      builder.setPort(0);
      httpService = builder.build();
      httpService.startAndWait();

      // Register services of test server
      discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return serviceName;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return httpService.getBindAddress();
        }
      });

      LOG.info("Started server on {}", httpService.getBindAddress());
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

    /**
     * Simple handler for server.
     */
    public class ServerHandler extends AbstractHttpHandler {
      private final Logger LOG = LoggerFactory.getLogger(ServerHandler.class);
      @GET
      @Path("/v1/ping/{text}")
      public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("text") String text) {
        numRequests.incrementAndGet();
        LOG.trace("Got text {}", text);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    }
  }
}
