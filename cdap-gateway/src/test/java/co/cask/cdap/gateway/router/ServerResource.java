/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.ChunkResponder;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * A generic server for testing router.
 */
public class ServerResource extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(ServerResource.class);

  static final int CHUNK_SIZE = 1024 * 1024;      // NOTE: maxUploadBytes % CHUNK_SIZE == 0

  private final String hostname;
  private final DiscoveryService discoveryService;
  private final String serviceName;
  private final AtomicInteger numRequests = new AtomicInteger(0);

  private NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private byte[] expectedJarBytes;

  ServerResource(String hostname, DiscoveryService discoveryService, String serviceName) {
    this.hostname = hostname;
    this.discoveryService = discoveryService;
    this.serviceName = serviceName;
  }

  @Override
  protected void before() throws Throwable {
    NettyRouterPipelineTest.GATEWAY_SERVER.clearNumRequests();

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
    LOG.info("Registering service {}", serviceName);
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(new Discoverable() {
      @Override
      public String getName() {
        return serviceName;
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

  public void setExpectedJarBytes(byte[] expectedJarBytes) {
    this.expectedJarBytes = expectedJarBytes;
  }

  /**
   * Simple handler for server.
   */
  public class ServerHandler extends AbstractHttpHandler {

    @GET
    @Path("/v1/echo/{name}")
    public void echo(HttpRequest request, final HttpResponder responder,
                     @PathParam("name") String name) throws InterruptedException, IOException {
      responder.sendString(HttpResponseStatus.OK, name);
    }

    @GET
    @Path("/v1/repeat/{name}")
    public void repeat(HttpRequest request, final HttpResponder responder,
                       @PathParam("name") String name) throws InterruptedException, IOException {
      responder.sendString(HttpResponseStatus.OK, name);
    }

    @POST
    @Path("/v1/upload")
    public void upload(HttpRequest request, final HttpResponder responder) throws InterruptedException, IOException {

      ChannelBuffer content = request.getContent();

      int readableBytes;
      int bytesRead = 0;
      ChunkResponder chunkResponder = responder.sendChunkStart(HttpResponseStatus.OK,
                                                               ImmutableMultimap.<String, String>of());
      while ((readableBytes = content.readableBytes()) > 0) {
        int read = Math.min(readableBytes, CHUNK_SIZE);
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

          if (Bytes.compareTo(expectedJarBytes, outputStream.toByteArray()) == 0) {
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
