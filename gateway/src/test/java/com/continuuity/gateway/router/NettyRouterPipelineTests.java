package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import junit.framework.Assert;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verify the ordering of events in the RouterPipeline.
 */
public class NettyRouterPipelineTests {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterTest.class);
  private static final String hostname = "127.0.0.1";
  private static final DiscoveryService discoveryService = new InMemoryDiscoveryService();
  private static final String gatewayService = Constants.Service.GATEWAY;
  private static final String webappService = "$HOST";

  private static final Supplier<String> gatewayServiceSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return gatewayService;
    }
  };

  public static final RouterResource router = new RouterResource(hostname, discoveryService,
                                                                 ImmutableSet.of("0:" + gatewayService,
                                                                                 "0:" + webappService));

  public static final ServerResource gatewayServer = new ServerResource(hostname, discoveryService,
                                                                         gatewayServiceSupplier);

  @SuppressWarnings("UnusedDeclaration")
  @ClassRule
  public static TestRule chain = RuleChain.outerRule(router).around(gatewayServer);

  @Before
  public void clearNumRequests() throws Exception {
    gatewayServer.clearNumRequests();

    // Wait for both servers of gatewayService to be registered
    Iterable<Discoverable> discoverables = ((DiscoveryServiceClient) discoveryService).discover(
      gatewayServiceSupplier.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  @Test
  public void testOrderingOfevents() throws Exception {

    // Send events to the socket to sleep for n seconds (passed in the path)
    // Verify that the order is maintained.

    Socket socket = new Socket("localhost",
                               router.getServiceMap().get(gatewayService));

    PrintWriter request = new PrintWriter( socket.getOutputStream() );

    request.write("GET /v1/ping/5 HTTP/1.1\r\n" +
                    " Host: localhost\r\n Connection: close\r\n\r\n"
    );

    request.write("GET /v1/ping/1 HTTP/1.1\r\n" +
                    " Host: localhost\r\n Connection: close\r\n\r\n"
    );
    request.flush();

    InputStream inStream = socket.getInputStream();
    int bufSize = socket.getSendBufferSize();
    byte[] buff = new byte[bufSize];

    // TODO: Some errors reading the buffer. Need to fix. For now I am able to verify the ordering in the pipeline
    // by seeing the Ping: 5 appear before Ping: 1
    inStream.read(buff);

    String line =  new String(buff, Charsets.UTF_8);
    Assert.assertTrue(line.contains("Ping:5"));
    LOG.info(line);

    //Verify gateway got both requests
    Assert.assertEquals(2, gatewayServer.getNumRequests());

    request.close();
    inStream.close();
    socket.close();

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
      cConf.set(Constants.Router.ADDRESS, hostname);
      cConf.setStrings(Constants.Router.FORWARD, forwards.toArray(new String[forwards.size()]));
      router =
        new NettyRouter(cConf, InetAddresses.forString(hostname),
                        new RouterServiceLookup((DiscoveryServiceClient) discoveryService));
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
    private static final Logger LOG = LoggerFactory.getLogger(ServerResource.class);

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
      gatewayServer.clearNumRequests();

      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableSet.of(new ServerHandler()));
      builder.setHost(hostname);
      builder.setPort(0);
      httpService = builder.build();
      httpService.startAndWait();

      registerServer();

      LOG.info("Started test server on {}", httpService.getBindAddress());
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
      LOG.info("Registering service {}", serviceNameSupplier.get());
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
      private final Logger LOG = LoggerFactory.getLogger(ServerHandler.class);
      @GET
      @Path("/v1/ping/{sleepInterval}")
      public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, final HttpResponder responder,
                       @PathParam("sleepInterval") String sleepInterval) {
        numRequests.incrementAndGet();
        try {
          TimeUnit.SECONDS.sleep(Long.valueOf(sleepInterval));
          //System.out.println(sleepInterval);
          responder.sendString(HttpResponseStatus.OK, "Ping:" + sleepInterval);
        } catch (InterruptedException e) {
          responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
         }
      }

    }
  }
}
