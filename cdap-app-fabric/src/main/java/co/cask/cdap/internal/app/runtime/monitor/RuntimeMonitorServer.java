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

package co.cask.cdap.internal.app.runtime.monitor;

import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.tools.HttpsEnabler;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Runtime Server which starts netty-http service to expose metadata to {@link RuntimeMonitor}
 */
public class RuntimeMonitorServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorServer.class);

  private final CConfiguration cConf;
  private final MultiThreadMessagingContext messagingContext;
  private final CountDownLatch shutdownLatch;
  private final Cancellable programRunCancellable;
  private final KeyStore keyStore;
  private final KeyStore trustStore;
  private NettyHttpService httpService;

  @Inject
  RuntimeMonitorServer(CConfiguration cConf, MessagingService messagingService,
                       Cancellable programRunCancellable,
                       @Constants.AppFabric.KeyStore KeyStore keyStore,
                       @Constants.AppFabric.TrustStore KeyStore trustStore) {
    this.cConf = cConf;
    this.messagingContext = new MultiThreadMessagingContext(messagingService);
    this.shutdownLatch = new CountDownLatch(1);
    this.programRunCancellable = programRunCancellable;
    this.keyStore = keyStore;
    this.trustStore = trustStore;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.RUNTIME_HTTP));
    InetSocketAddress address = getServerSocketAddress(cConf);

    // Enable SSL for communication.
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME_HTTP)
      .setHttpHandlers(new RuntimeHandler(cConf, messagingContext))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(address.getHostName())
      .setPort(address.getPort());

    httpService = new HttpsEnabler()
      .setKeyStore(keyStore, ""::toCharArray)
      .setTrustStore(trustStore)
      .enable(builder)
      .build();

    httpService.start();
    LOG.info("Runtime monitor server started on {}", httpService.getBindAddress());
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }

  @Override
  protected void shutDown() throws Exception {
    // Cancel the program run if it is running. The implementation of the cancellable should handle the details.
    try {
      programRunCancellable.cancel();
    } catch (Exception e) {
      LOG.error("Exception raised when stopping program run.", e);
    }

    // Wait for the shutdown signal from the runtime monitor before shutting off the http server.
    // This allows the runtime monitor still able to talk to this service until all data are fetched.
    Uninterruptibles.awaitUninterruptibly(shutdownLatch);
    httpService.stop();
    LOG.info("Runtime monitor server stopped");
  }

  /**
   * Returns the {@link InetSocketAddress} for the http service to bind to.
   */
  private InetSocketAddress getServerSocketAddress(CConfiguration cConf) {
    String host = cConf.get(Constants.RuntimeMonitor.SERVER_HOST);
    if (host == null) {
      host = InetAddress.getLoopbackAddress().getCanonicalHostName();
    }
    int port = cConf.getInt(Constants.RuntimeMonitor.SERVER_PORT);
    return new InetSocketAddress(host, port);
  }

  /**
   * {@link co.cask.http.HttpHandler} for exposing metadata of a runtime.
   */
  @Path("/v1/runtime")
  public final class RuntimeHandler extends AbstractHttpHandler {

    private final CConfiguration cConf;
    private final MessagingContext messagingContext;

    RuntimeHandler(CConfiguration cConf, MessagingContext messagingContext) {
      this.cConf = cConf;
      this.messagingContext = messagingContext;
    }

    /**
     * Gets list of topics along with offsets and limit as request and returns list of messages
     */
    @POST
    @Path("/metadata")
    public void metadata(FullHttpRequest request, HttpResponder responder) throws Exception {
      Map<String, GenericRecord> consumeRequests = decodeConsumeRequest(request);
      MessagesBodyProducer messagesBodyProducer = new MessagesBodyProducer(cConf, consumeRequests, messagingContext);
      responder.sendContent(HttpResponseStatus.OK, messagesBodyProducer,
                            new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE, "avro/binary"));
    }

    /**
     * Shuts down the runtime monitor server.
     */
    @POST
    @Path("/shutdown")
    public void shutdown(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "Triggering shutdown down Runtime Http Server.");
      shutdownLatch.countDown();
      stop();
    }

    /**
     * Kills the running program.
     */
    @POST
    @Path("/kill")
    public void kill(HttpRequest request, HttpResponder responder) {
      programRunCancellable.cancel();
      responder.sendString(HttpResponseStatus.OK, "Program killed.");
    }

    /**
     * Decode consume request from avro binary format
     */
    private Map<String, GenericRecord> decodeConsumeRequest(FullHttpRequest request) throws IOException {
      Decoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteBufInputStream(request.content()), null);
      DatumReader<Map<String, GenericRecord>> datumReader = new GenericDatumReader<>(
        MonitorSchemas.V1.MonitorConsumeRequest.SCHEMA);
      return datumReader.read(null, decoder);
    }
  }
}
