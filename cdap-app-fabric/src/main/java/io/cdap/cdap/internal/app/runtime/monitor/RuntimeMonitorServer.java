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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
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
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Runtime Server which starts netty-http service to expose metadata to {@link RuntimeMonitor}.
 * It also starts a {@link TrafficRelayServer} for relaying service traffic to CDAP master services.
 */
public class RuntimeMonitorServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorServer.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final CountDownLatch shutdownLatch;
  private final Cancellable programRunCancellable;
  private final Authenticator authenticator;
  private final NettyHttpService httpService;
  private final long shutdownTimeoutSeconds;

  @Inject
  RuntimeMonitorServer(CConfiguration cConf, MessagingService messagingService,
                       Cancellable programRunCancellable, Authenticator authenticator,
                       @Constants.AppFabric.KeyStore KeyStore keyStore,
                       @Constants.AppFabric.TrustStore KeyStore trustStore) {
    this.cConf = cConf;
    this.shutdownLatch = new CountDownLatch(1);
    this.programRunCancellable = programRunCancellable;
    this.authenticator = authenticator;
    this.shutdownTimeoutSeconds = cConf.getLong("system.runtime.monitor.retry.policy.max.time.secs");

    // Creates the http service
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME)
      .setHttpHandlers(new RuntimeHandler(cConf, new MultiThreadMessagingContext(messagingService)))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(InetAddress.getLoopbackAddress().getHostName());

    // Enable SSL for communication.
    this.httpService = new HttpsEnabler()
      .setKeyStore(keyStore, ""::toCharArray)
      .setTrustStore(trustStore)
      .enable(builder)
      .build();
  }

  @Override
  protected void startUp() throws Exception {
    httpService.start();

    // Writes the port to a local file
    Retries.runWithRetries(
      () -> {
        String content = GSON.toJson(new RuntimeMonitorServerInfo(httpService.getBindAddress()));
        java.nio.file.Path infoFile = Paths.get(cConf.get(Constants.RuntimeMonitor.SERVER_INFO_FILE));
        Files.deleteIfExists(infoFile);
        Files.move(Files.write(Files.createTempFile(infoFile.getFileName().toString(), ".tmp"),
                               Collections.singletonList(content)),
                   infoFile);
      },
      RetryStrategies.fixDelay(1, TimeUnit.SECONDS), IOException.class::isInstance);

    LOG.info("Runtime monitor server started on {}", httpService.getBindAddress());

    // Set the authenticator
    Authenticator.setDefault(authenticator);
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

    Authenticator.setDefault(null);

    // Wait for the shutdown signal from the runtime monitor before shutting off the http server.
    // This allows the runtime monitor still able to talk to this service until all data are fetched.
    if (!Uninterruptibles.awaitUninterruptibly(shutdownLatch, shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
      LOG.warn("Did not receive a shutdown signal from the master after {} seconds, proceeding with shutdown. "
                 + "This may result in missing logs or metrics.", shutdownTimeoutSeconds);
    }
    httpService.stop();
    LOG.info("Runtime monitor server stopped");
  }

  /**
   * {@link io.cdap.http.HttpHandler} for exposing metadata of a runtime.
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
