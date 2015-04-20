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

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.gateway.auth.NoAuthenticator;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.guice.SecurityModules;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
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
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Verify the ordering of events in the RouterPipeline.
 */
public class NettyRouterPipelineTest {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRouterPipelineTest.class);
  private static final String hostname = "127.0.0.1";
  private static final DiscoveryService discoveryService = new InMemoryDiscoveryService();
  private static final String gatewayService = Constants.Service.APP_FABRIC_HTTP;
  private static final String GATEWAY_LOOKUP = Constants.Router.GATEWAY_DISCOVERY_NAME;
  private static final String webappService = "$HOST";
  private static final int maxUploadBytes = 10 * 1024 * 1024;
  private static final int chunkSize = 1024 * 1024;      // NOTE: maxUploadBytes % chunkSize == 0
  private static byte[] applicationJarInBytes;

  private static final Supplier<String> gatewayServiceSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return gatewayService;
    }
  };

  public static final RouterResource ROUTER = new RouterResource(hostname, discoveryService);

  public static final ServerResource GATEWAY_SERVER = new ServerResource(hostname, discoveryService,
                                                                         gatewayServiceSupplier);

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @SuppressWarnings("UnusedDeclaration")
  @ClassRule
  public static TestRule chain = RuleChain.outerRule(ROUTER).around(GATEWAY_SERVER);

  @Before
  public void clearNumRequests() throws Exception {
    GATEWAY_SERVER.clearNumRequests();

    // Wait for both servers of gatewayService to be registered
    Iterable<Discoverable> discoverables = ((DiscoveryServiceClient) discoveryService).discover(
      gatewayServiceSupplier.get());
    for (int i = 0; i < 50 && Iterables.size(discoverables) != 1; ++i) {
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  @Test
  public void testChunkRequestSuccess() throws Exception {

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();

    final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(
      new NettyAsyncHttpProvider(configBuilder.build()),
      configBuilder.build());

    byte [] requestBody = generatePostData();
    final Request request = new RequestBuilder("POST")
      .setUrl(String.format("http://%s:%d%s", hostname, ROUTER.getServiceMap().get(GATEWAY_LOOKUP), "/v1/upload"))
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
  public void testDeployNTimes() throws Exception {
    // regression tests for race condition during multiple deploys.
    deploy(100);
  }

  //Deploy word count app n times.
  private void deploy(int num) throws Exception {

    String path = String.format("http://%s:%d/v1/deploy",
                                hostname,
                                ROUTER.getServiceMap().get(GATEWAY_LOOKUP));

    LocationFactory lf = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location programJar = AppJarHelper.createDeploymentJar(lf, AllProgramsApp.class);

    applicationJarInBytes = ByteStreams.toByteArray(Locations.newInputSupplier(programJar));
    for (int i = 0; i < num; i++) {
      LOG.info("Deploying {}/{}", i, num);
      URL url = new URL(path);
      HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
      urlConn.setRequestProperty("X-Archive-Name", "Purchase-1.0.0.jar");
      urlConn.setRequestMethod("POST");
      urlConn.setDoOutput(true);
      urlConn.setDoInput(true);

      ByteStreams.copy(Locations.newInputSupplier(programJar), urlConn.getOutputStream());
      Assert.assertEquals(200, urlConn.getResponseCode());
      urlConn.disconnect();
    }
  }

  private static class RouterResource extends ExternalResource {
    private final String hostname;
    private final DiscoveryService discoveryService;
    private final Map<String, Integer> serviceMap = Maps.newHashMap();

    private NettyRouter router;

    private RouterResource(String hostname, DiscoveryService discoveryService) {
      this.hostname = hostname;
      this.discoveryService = discoveryService;
    }

    @Override
    protected void before() throws Throwable {
      CConfiguration cConf = CConfiguration.create();
      Injector injector = Guice.createInjector(new ConfigModule(cConf), new IOModule(),
                                               new SecurityModules().getInMemoryModules(),
                                               new DiscoveryRuntimeModule().getInMemoryModules());
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      AccessTokenTransformer accessTokenTransformer = injector.getInstance(AccessTokenTransformer.class);
      SConfiguration sConf = injector.getInstance(SConfiguration.class);
      cConf.set(Constants.Router.ADDRESS, hostname);
      cConf.setInt(Constants.Router.ROUTER_PORT, 0);
      cConf.setInt(Constants.Router.WEBAPP_PORT, 0);
      router =
        new NettyRouter(cConf, sConf, InetAddresses.forString(hostname),
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
      GATEWAY_SERVER.clearNumRequests();

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
      cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
        @Override
        public String getName() {
          return serviceNameSupplier.get();
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return httpService.getBindAddress();
        }
      }));
    }

    public void cancelRegistration() {
      cancelDiscovery.cancel();
    }

    /**
     * Simple handler for server.
     */
    public class ServerHandler extends AbstractHttpHandler {
      private final Logger log = LoggerFactory.getLogger(ServerHandler.class);
      @POST
      @Path("/v1/upload")
      public void upload(HttpRequest request, final HttpResponder responder) throws InterruptedException, IOException {
        ChannelBuffer content = request.getContent();

        int readableBytes;
        int bytesRead = 0;
        ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                                                 ImmutableMultimap.<String, String>of());
        while ((readableBytes = content.readableBytes()) > 0) {
          int read = Math.min(readableBytes, chunkSize);
          bytesRead += read;
          chunkResponder.sendChunk(content.readSlice(read));
          //TimeUnit.MILLISECONDS.sleep(RANDOM.nextInt(1));
        }
        chunkResponder.close();
      }

      @POST
      @Path("/v1/deploy")
      public BodyConsumer deploy(HttpRequest request, final HttpResponder responder) throws InterruptedException {
        return new BodyConsumer() {
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          int count = 0;
          @Override
          public void chunk(ChannelBuffer request, HttpResponder responder) {
            count += request.readableBytes();
            if (request.readableBytes() > 0) {
            }
            outputStream.write(request.array(), 0, request.readableBytes());
          }

          @Override
          public void finished(HttpResponder responder) {

            if (Bytes.compareTo(applicationJarInBytes, outputStream.toByteArray()) == 0) {
              responder.sendStatus(HttpResponseStatus.OK);
              return;
            }
              responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }

          @Override
          public void handleError(Throwable cause) {
            throw Throwables.propagate(cause);
          }
        };
      }

    }
  }

  private static class ByteEntityWriter implements Request.EntityWriter {
    private final byte [] bytes;

    private ByteEntityWriter(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public void writeEntity(OutputStream out) throws IOException {
      for (int i = 0; i < maxUploadBytes; i += chunkSize) {
        out.write(bytes, i, chunkSize);
      }
    }
  }

  private static byte [] generatePostData() {
    byte [] bytes = new byte [maxUploadBytes];

    for (int i = 0; i < maxUploadBytes; ++i) {
      bytes[i] = (byte) i;
    }

    return bytes;
  }

}
